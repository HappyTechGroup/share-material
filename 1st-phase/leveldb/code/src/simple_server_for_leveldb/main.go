package main

import (
    "log"
    "encoding/json"
    "net/http"
    "code.google.com/p/leveldb-go/leveldb/db"
    "code.google.com/p/leveldb-go/leveldb"
)

var dbConn db.DB
var err error
var opts *db.Options = &db.Options{}

type Response struct {
    Status string
    Msg string
}

func genResponseStr(status string, message string) []byte {
    resp := Response {
        Status: status,
        Msg: message,
    }

    responseContent, _ := json.MarshalIndent(resp, "", "    ")
    return responseContent
}

func RequestHandler(w http.ResponseWriter, req *http.Request) {

    w.Header().Set("Content-Type", "application/json")

    log.Println("Request Method: ", req.Method)

    err := req.ParseMultipartForm(1024 * 1024)
    if err != nil {
        log.Println("Failed to parse form", err)
        w.Write(genResponseStr("500", "请求无效！"))
        return
    }

    key := req.FormValue("key")
    log.Println("Key: ", key)
    if key == "" {
        log.Println("key参数为空或不存在")
        w.Write(genResponseStr("500", "提供key请求参数且不能为空"))
        return
    }

    if req.Method == "GET" {
        targetValue, err := dbConn.Get([]byte(key), nil)
        if err != nil {
            w.Write(genResponseStr("500", err.Error()))
            return
        }
        w.Write(genResponseStr("200", string(targetValue)))
        return
    }

    if req.Method == "DELETE" {
        err = dbConn.Delete([]byte(key), nil)
        if err != nil {
            w.Write(genResponseStr("500", err.Error()))
            return
        }
        w.Write(genResponseStr("200", "成功"))
        return
    }

    if req.Method == "POST" {
        value := req.FormValue("value")
        log.Println("Value: ", value)
        err = dbConn.Set([]byte(key), []byte(value), nil)
        if err != nil {
            w.Write(genResponseStr("500", err.Error()))
            return
        }
        w.Write(genResponseStr("200", "成功"))
        return
    }

    w.Write(genResponseStr("500", "不存在此操作"))
    return
}

func main() {
    dbConn, err = leveldb.Open("./data", opts)
    if err != nil {
       log.Fatal("Failed to open db dir ./data", err)
       return
    }

    http.HandleFunc("/leveldb/", RequestHandler)
	err = http.ListenAndServe(":8799", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
