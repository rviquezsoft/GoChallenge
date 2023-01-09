package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	//"strings"

	//"sync"
	"time"
	//"log"
	//"strings"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hola mundo!")
	})

	start := time.Now()

	homeDir, _ := os.UserHomeDir()
	documentsDir := filepath.Join(homeDir, "Documents/", dirName)

	entries, _ := ioutil.ReadDir(documentsDir)

	processDirs(entries, documentsDir)

	elapsed := time.Since(start)

	fmt.Printf("Duration: %s\n", elapsed)

	http.HandleFunc(":8090/debug/pprof/", pprof.Index)

	err := http.ListenAndServe(":8089", nil)
	if err != nil {
		fmt.Println(err)
	}

	err2 := http.ListenAndServe("localhost:6060", nil)
	if err2 != nil {
		fmt.Println(err2)
	}

}

func processDirs(entries []os.FileInfo, documentsDir string) {

	for _, entry := range entries {
		if entry.IsDir() {
			route := filepath.Join(documentsDir, entry.Name())
			subs, err := ioutil.ReadDir(route)
			if err != nil {
				fmt.Println(err)
			}
			for _, fi := range subs {
				if fi.IsDir() {
					finalRoute := filepath.Join(documentsDir, entry.Name(), fi.Name())
					dir, err := os.Open(finalRoute)
					if err != nil {
						fmt.Println(err)
					}
					defer dir.Close()

					fmt.Println(finalRoute)

					indexGenerated := entry.Name() + "_____" + fi.Name()
					indexPayload := map[string]interface{}{
						"name":         indexGenerated,
						"storage_type": "disk",
						"shard_num":    1,
						"mappings": map[string]interface{}{
							"properties": map[string]interface{}{
								"user": map[string]interface{}{
									"type":          "text",
									"index":         true,
									"store":         true,
									"highlightable": true,
								},
								"folderName": map[string]interface{}{
									"type":          "text",
									"index":         true,
									"store":         true,
									"highlightable": true,
								},
								"file": map[string]interface{}{
									"type":          "text",
									"index":         true,
									"store":         true,
									"highlightable": true,
								},
								"content": map[string]interface{}{
									"type":          "text",
									"index":         false,
									"store":         true,
									"highlightable": true,
								},
							},
						},
					}
					indexBody, err := sendPostRequest(userApi, passwordApi, indexEndpoint, indexPayload)
					fmt.Println(string(indexBody))

					files, err := dir.Readdir(-1)
					if err != nil {
						fmt.Println(err)
						return
					}

					var jsonDataList []map[string]string

					for _, file := range files {
						fmt.Println(filepath.Join(finalRoute, file.Name()))
						content, err := ioutil.ReadFile(filepath.Join(finalRoute, file.Name()))
						if err != nil {
							fmt.Println(err)
							return
						}
						jsonData := map[string]string{
							"user":       entry.Name(),
							"folderName": fi.Name(),
							"file":       file.Name(),
							"content":    string(content),
						}
						//Agregar el jsonData al slice
						jsonDataList = append(jsonDataList, jsonData)
						//fmt.Println(string(content))
					}

					jsonBytes, err := json.MarshalIndent(jsonDataList, "", "  ")
					if err != nil {
						fmt.Println("error en MarshalIndent")
					}

					fmt.Println(string(jsonBytes))

					jsonPayload := map[string]interface{}{
						"index":   indexGenerated,
						"records": jsonDataList,
					}
		
					bulkBody, err := sendPostRequest(userApi, passwordApi, documentBulkEndpoint, jsonPayload)
					if err != nil {
						fmt.Println("error en sendPostRequest jsonPayload")
					}
		
					fmt.Println(string(bulkBody))
				}
			}
		}
	}
}

const dirName = "enron_mail_20110402/maildir"
const urlBase = "http://localhost:4080/"
const indexName = "emails_index"
const docName = "emails_doc"

var indexEndpoint = urlBase + "api/index"
var documentEndpoint = urlBase + "api/" + indexName + "/_doc"
var documentBulkEndpoint = urlBase + "api/_bulkv2"

const userApi = "admin"
const passwordApi = "Complexpass#123"

var docPayload = map[string]interface{}{
	"name": docName,
}

func sendPostRequest(username, password, url string, data interface{}) ([]byte, error) {
	//Convierte el cuerpo de la solicitud en formato JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	//Crea una nueva solicitud POST con el cuerpo en formato JSON
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	//Establece la autenticación básica en la solicitud
	req.SetBasicAuth(username, password)

	//Establece el tipo de contenido en la solicitud como "application/json"
	req.Header.Set("Content-Type", "application/json")

	//Crea un nuevo cliente HTTP
	client := &http.Client{}

	//Envía la solicitud y obtiene la respuesta
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	//Lee el cuerpo de la respuesta
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}
