package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"
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
const OutputPath = "treehash.txt"
const WorkerCount = 1000

var waitWorkers sync.WaitGroup
var waitHashWorker sync.WaitGroup

var workQueue = make(chan workItem, WorkerCount)

type workItem struct {
	File string
	Info os.FileInfo
}

var hashQueue = make(chan hashItem, 100)

type hashItem struct {
	File string
	Hash []byte
	Info os.FileInfo
}

func worker() {
	buf := make([]byte, 102400)
	for item := range workQueue {
		fd, _ := os.Open(item.File)

		hash := sha1.New()
		io.CopyBuffer(hash, fd, buf)
		fd.Close()

		hashQueue <- hashItem{item.File, hash.Sum(nil), item.Info}
	}

	waitWorkers.Done()
}

func hashwriter(output string) {
	fd, _ := os.OpenFile(output, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer fd.Close()

	for item := range hashQueue {
		println(item.File)
		fmt.Fprintf(fd, "%s,%x,%d\n", item.File, item.Hash, item.Info.Size())
	}
	waitHashWorker.Done()
}

//func createWriter(output string) {
//	for node := range hashChanel {
//		file, err := os.Open(node.Path)
//		if err != nil {
//			fmt.Println(err.Error())
//			os.Exit(-1)
//		}
//		hash := sha1.New()
//		if _, ioErr := io.Copy(hash, file); ioErr != nil {
//			fmt.Println(ioErr.Error())
//			os.Exit(-1)
//		}
//
//		if closeErr := file.Close(); closeErr != nil {
//			fmt.Println(closeErr.Error())
//			os.Exit(-1)
//		}
//
//		fd, fileErr := os.OpenFile(output, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
//		if fileErr != nil {
//			fmt.Println("hash file error: ", fileErr.Error())
//			os.Exit(-1)
//		}
//
//		data := fmt.Sprintf("%s,%x,%d\n", node.Path, hash.Sum(nil), node.Size)
//		buf := []byte(data)
//		fd.Write(buf)
//		if fdErr := fd.Close(); fdErr != nil {
//			fmt.Println(fdErr.Error())
//			os.Exit(-1)
//		}
//	}
//}

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

	waitWorkers.Add(WorkerCount)
	for i := 0; i < WorkerCount; i++ {
		go worker()
	}

	waitHashWorker.Add(1)
	go hashwriter(output)

	// 遍历时，保存树中的结点
	filepath.Walk(rootpath, func(path string, info os.FileInfo, err error) error {
		if reg.Match([]byte(path)) {
			return filepath.SkipDir
		}

		workQueue <- workItem{path, info}
		return nil
	})

	close(workQueue)
	waitWorkers.Wait()
	close(hashQueue)
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

	runtime.GOMAXPROCS(runtime.NumCPU())

	if result := Traverse(*root, *filter, *output); result != Success {
		os.Exit(-1)
	}
	fmt.Println("duration: ", time.Now().Sub(beginTime).Seconds(), "s")
}
