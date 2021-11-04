/*
 	A little tool to extract documentation

	It processes two sources into rst files:
	- Source comments with the "+extract target" tag are added to target.rst
	- CLI help information into cli.rst

	It should be build and run in this folder, as it uses the relative path to
	find the OCP source code
*/
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func unindent(text string) string {

	// we assume the indent of the text is consinstant, so either tabs or spaces,
	// or in the same order if mixed. We also assume the first line has the minimal
	// indent. This allows to extraact it and remove it line by line

	if len(text) == 0 {
		return text
	}

	lines := strings.Split(text, "\n")
	spaceless := strings.TrimSpace(lines[0])
	trimmed := lines[0][:(len(lines[0]) - len(spaceless))]

	text = ""
	for _, line := range lines {
		text += strings.Replace(line, trimmed, "", 1) + "\n"
	}

	return text
}

func getCommentWithTarget(cgrp *ast.CommentGroup) (string, string, bool) {
	s := cgrp.Text()
	parts := strings.SplitN(s, "\n", 2)
	definer := strings.TrimSpace(parts[0])
	if strings.HasPrefix(definer, "+extract") {

		target := strings.TrimSpace(strings.TrimPrefix(definer, "+extract"))
		return target, unindent(parts[1]), true
	}
	return "", "", false
}

func getComments(pkg *ast.Package, comments map[string][]string) {

	for _, f := range pkg.Files {
		for _, c := range f.Comments {
			target, comment, ok := getCommentWithTarget(c)
			if ok {
				if _, has := comments[target]; !has {
					comments[target] = make([]string, 0)
				}
				comments[target] = append(comments[target], comment)
			}
		}
	}
}

func main() {

	//set the main logging output
	log.SetOutput(os.Stdout)

	mainpath, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	mainpath = filepath.Dir(mainpath)
	log.Printf("Start comment processing in %s", mainpath)

	comments := make(map[string][]string, 0)
	paths := []string{mainpath}

	files, err := ioutil.ReadDir(mainpath)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		if f.IsDir() {
			paths = append(paths, filepath.Join(mainpath, f.Name()))
		}
	}

	fset := token.NewFileSet()
	for _, path := range paths {
		pkgs, err := parser.ParseDir(fset, path, nil, parser.ParseComments)
		if err != nil {
			log.Fatalf("Error parsing files in %s: %s\n", path, err)
			os.Exit(1)
		}

		for _, pkg := range pkgs {
			log.Printf("Parsing package %s", pkg.Name)
			getComments(pkg, comments)
		}
	}

	for target, texts := range comments {

		filename := target + ".rst"
		log.Printf("Writing %s", filename)
		folderpath := filepath.Join(mainpath, "docs", "source", "generated")
		fpath := filepath.Join(folderpath, filename)

		if _, err := os.Stat(fpath); err == nil {
			os.Remove(fpath)
		}

		file, err := os.Create(fpath)
		if err != nil {
			log.Fatalf("Error creating output file %s: %s\n", filename, err)
		}

		for _, text := range texts {
			fmt.Fprintln(file, text)
		}

		file.Close()
	}

	log.Println("Done comment processing")
}
