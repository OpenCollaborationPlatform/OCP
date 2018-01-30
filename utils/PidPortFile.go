// PidPortFile
package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/facebookgo/atomicfile"
	"github.com/spf13/viper"
)

// Write the pidfile based on the flag. It is an error if the pidfile hasn't
// been configured.
func WritePidPort() error {

	dir := viper.GetString("directory")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return errors.New("OCP directory not configured: did you call init?")
	}
	path := filepath.Join(dir, "pidport")

	if err := os.MkdirAll(filepath.Dir(path), os.FileMode(0755)); err != nil {
		return err
	}

	file, err := atomicfile.New(path, os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("error opening pidfile %s: %s", path, err)
	}
	defer file.Close() // in case we fail before the explicit close

	_, err = fmt.Fprintf(file, "%d:%v", os.Getpid(), viper.GetInt("connection.port"))
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

// Read the pid from the configured file. It is an error if the pidfile hasn't
// been configured.
func ReadPidPort() (int, int, error) {

	dir := viper.GetString("directory")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return 0, 0, errors.New("OCP directory not configured: did you call init?")
	}
	path := filepath.Join(dir, "pidport")

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return -1, -1, nil
	}

	d, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}

	strs := strings.Split(string(bytes.TrimSpace(d)), ":")
	if len(strs) != 2 {

	}
	pid, err := strconv.Atoi(strs[0])
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing pid from %s: %s", path, err)
	}

	port, err := strconv.Atoi(strs[1])
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing port from %s: %s", path, err)
	}

	return pid, port, nil
}

func ClearPidPort() error {

	dir := viper.GetString("directory")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return errors.New("OCP directory not configured: did you call init?")
	}
	path := filepath.Join(dir, "pidport")

	err := os.Remove(path)
	if err != nil {
		fmt.Printf("Error removing pid file: %s", err)
	}
	return err
}
