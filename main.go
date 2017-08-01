package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"sync"
	"time"

	"path/filepath"

	"github.com/xiaonanln/go-xnsyncutil/xnsyncutil"
)

const (
	// Success 成功
	Success = iota

	// PathNullErr 路径为空
	PathNullErr

	// InvalidPathErr 不是有效的路径
	InvalidPathErr

	// FileNotDIR 是文件，而不是目录
	FileNotDIR

	// OutputPathErr 输出文件的路径错误
	OutputPathErr

	// PermissionErr 权限错误
	PermissionErr

	// NoChildrenErr 即没有子目录，也没有文件
	NoChildrenErr
)

// OutputPath 默认的输出文件路径
const (
	OutputPath     = "treehash.txt"
	WorkerCount    = 10000
	DirWalkerCount = 10000
)

var waitDirWalkers sync.WaitGroup
var waitWorkers sync.WaitGroup
var waitHashWorker sync.WaitGroup

var dirWalkQueue = xnsyncutil.NewSyncQueue()

type dirWalkItem struct {
	Path string
}

var fileWorkQueue = make(chan fileWorkItem, WorkerCount*10)

type fileWorkItem struct {
	File string
	Info os.FileInfo
}

var hashWorkQueue = make(chan haskWorkItem, 1000)

type haskWorkItem struct {
	File string
	Hash []byte
	Info os.FileInfo
}

func dirWalker(reg *regexp.Regexp) {
	for {
		_item := dirWalkQueue.Pop()
		if _item == nil {
			break
		}

		item := _item.(dirWalkItem)
		println(item.Path)
		files, _ := ioutil.ReadDir(item.Path)
		for _, f := range files {
			fpath := filepath.Join(item.Path, f.Name())
			if reg.Match([]byte(f.Name())) {
				continue
			}

			if f.IsDir() {
				dirWalkQueue.Push(dirWalkItem{fpath})
				waitDirWalkers.Add(1)
			} else {
				fileWorkQueue <- fileWorkItem{fpath, f}
			}
		}

		waitDirWalkers.Done()
	}
}

func worker() {
	buf := make([]byte, 102400)
	for item := range fileWorkQueue {
		fd, _ := os.Open(item.File)

		hash := sha1.New()
		io.CopyBuffer(hash, fd, buf)
		fd.Close()

		hashWorkQueue <- haskWorkItem{item.File, hash.Sum(nil), item.Info}
	}

	waitWorkers.Done()
}

func hashwriter(output string) {
	fd, _ := os.OpenFile(output, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer fd.Close()

	for item := range hashWorkQueue {
		//println(item.File)
		fmt.Fprintf(fd, "%s,%x,%d\n", item.File, item.Hash, item.Info.Size())
	}
	waitHashWorker.Done()
}

// Traverse 遍历目录
func Traverse(rootpath string, filter string, output string) int {
	if rootpath == "" {
		fmt.Println("root参数不能为空")
		displayHelpCMD()
		return PathNullErr
	}
	rootDir, err := os.Stat(rootpath)
	if err != nil {
		fmt.Println(rootpath + " 不是有效的目录")
		displayHelpCMD()
		return InvalidPathErr
	}
	if !rootDir.IsDir() {
		fmt.Println(rootpath + " 不是目录")
		displayHelpCMD()
		return FileNotDIR
	}

	if output != "" {
		output = OutputPath
	}

	var reg *regexp.Regexp
	var regErr error

	if filter != "" {
		if reg, regErr = regexp.Compile(filter); regErr != nil {
			reg = nil
		}
	}

	dirWalkQueue.Push(dirWalkItem{rootpath})
	waitDirWalkers.Add(1)
	for i := 0; i < DirWalkerCount; i++ {
		go dirWalker(reg)
	}

	waitWorkers.Add(WorkerCount)
	for i := 0; i < WorkerCount; i++ {
		go worker()
	}

	waitHashWorker.Add(1)
	go hashwriter(output)

	//// 遍历时，保存树中的结点
	//filepath.Walk(rootpath, func(path string, info os.FileInfo, err error) error {
	//	if reg.Match([]byte(path)) {
	//		return filepath.SkipDir
	//	}
	//
	//	fileWorkQueue <- fileWorkItem{path, info}
	//	return nil
	//})
	waitDirWalkers.Wait()

	close(fileWorkQueue)
	waitWorkers.Wait()
	close(hashWorkQueue)
	waitHashWorker.Wait()
	return Success
}

func displayHelpCMD() {
	fmt.Println("运行以下命令获得帮助")
	fmt.Println("go run main.go help")
}

func displayHelp() {
	fmt.Println("*********************************************")
	fmt.Println("*  参数说明:                                *")
	fmt.Println("*  -root", "要计算hash的根目录                 *")
	fmt.Println("*  -filter", "需要过滤的目录或文件，支持通配符 *")
	fmt.Println("*  -output", "最后写入的文件路径               *")
	fmt.Println("*********************************************")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	beginTime := time.Now()
	root := flag.String("root", "", "要生成hash树的根目录")
	filter := flag.String("filter", "", "过滤目录或文件，支持通配符")
	output := flag.String("output", "", "最后写入的文件路径")

	flag.Parse()

	args := flag.Args()
	hasHelp := false
	if len(args) >= 1 {
		for _, value := range args {
			if value == "help" {
				hasHelp = true
				displayHelp()
				break
			}
		}
	}

	if hasHelp && *root == "" {
		os.Exit(0)
	}

	if result := Traverse(*root, *filter, *output); result != Success {
		os.Exit(-1)
	}
	fmt.Println("duration: ", time.Now().Sub(beginTime).Seconds(), "s")
}
