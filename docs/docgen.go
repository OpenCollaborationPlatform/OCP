/*
 	A little tool to extract documentation

	It processes two sources into rst files:
	- Source comments with the "+extract" tag are added to packagename.rst
	  - ordering of comments can be manipulated by adding prio:num (same prio
	    comments are ordered by order of encounter)
	  - adding indent can be achieved by indent:num (num being the number
	    of tabs)

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
	"sort"
	"strconv"
	"strings"
	"unicode"
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

func tabIndent(text string, tabs int) string {

	tapsStr := strings.Repeat("\t", tabs)

	result := ""
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		result += tapsStr + line + "\n"
	}
	return result
}

func getCommentWithPrio(cgrp *ast.CommentGroup) (int, string, bool) {
	s := cgrp.Text()
	parts := strings.SplitN(s, "\n", 2)
	definer := strings.TrimSpace(parts[0])
	if strings.HasPrefix(definer, "+extract") {

		text := unindent(parts[1])

		f := func(c rune) bool {
			return !unicode.IsLetter(c) && !unicode.IsNumber(c)
		}
		fields := strings.FieldsFunc(definer, f)

		prio := 99
		indent := 0
		for i, field := range fields {
			switch field {
			case "prio":
				if val, err := strconv.Atoi(fields[i+1]); err == nil {
					prio = val
				} else {
					log.Fatalln("prio argument in extracted comment must be integer")
				}
			case "indent":
				if val, err := strconv.Atoi(fields[i+1]); err == nil {
					indent = val
				} else {
					log.Fatalln("tabs argument in extracted comment must be integer")
				}
			}
		}

		if indent != 0 {
			text = tabIndent(text, indent)
		}
		return prio, text, true
	}
	return 0, "", false
}

func sortFiles(files map[string]*ast.File) []*ast.File {

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make([]*ast.File, 0, len(files))
	for _, name := range names {
		result = append(result, files[name])
	}

	return result
}

func getComments(pkg *ast.Package) []string {

	files := sortFiles(pkg.Files)
	prioComments := make(map[int][]string)
	for _, f := range files {
		for _, c := range f.Comments {
			prio, comment, ok := getCommentWithPrio(c)
			if ok {
				if _, has := prioComments[prio]; !has {
					prioComments[prio] = make([]string, 0)
				}
				prioComments[prio] = append(prioComments[prio], comment)
			}
		}
	}

	// sort priorities and append all comments in correct order
	prios := make([]int, 0, len(prioComments))
	for prio := range prioComments {
		prios = append(prios, prio)
	}
	sort.Ints(prios)

	result := make([]string, 0)
	for _, prio := range prios {
		result = append(result, prioComments[prio]...)
	}
	return result
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
			result := getComments(pkg)
			if len(result) != 0 {
				comments[pkg.Name] = result
			}
		}
	}

	for target, texts := range comments {

		filename := target + ".rst"
		log.Printf("Writing %s", filename)
		fpath := filepath.Join(mainpath, "docs", "source", filename)

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
