package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	_ "github.com/go-sql-driver/mysql"
)

const FIELD = "Field"

type TableRow struct {
	Content map[sql.NullString]interface{}
}

type FieldStruct struct {
	Struct map[string]interface{}
}
type Table struct {
	Fields  map[string]*FieldStruct
	FieldId string
}

type Schema struct {
	Tables map[string]*Table
}

type Handler struct {
	DB     *sql.DB
	Schema *Schema
}

type SQLRows struct {
	rows *sql.Rows
}

type Result map[string]interface{}

func ReadTableNullString(db *sql.DB, rows *sql.Rows, tableStruct map[string]*FieldStruct) ([]map[string]interface{}, error) {
	res := []map[string]interface{}{}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colStrs := make([]string, len(cols))
	for i, col := range cols {
		colStrs[i] = col
	}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		dict := make(map[string]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			vals[i] = new(sql.NullString)
		}
		if err := rows.Scan(vals...); err != nil {
			return nil, err
		}
		for i, col := range colStrs {
			value := *(vals[i].(*sql.NullString))
			if value.Valid {
				if tableStruct != nil {
					if tableStruct[col].Struct["Type"] == "int" {
						dict[col], err = strconv.Atoi(value.String)
						if err != nil {
							return nil, err
						}
					} else if tableStruct[col].Struct["Type"] == "float" {
						dict[col], err = strconv.ParseFloat(value.String, 64)
						if err != nil {
							return nil, err
						}
					} else {
						dict[col] = value.String
					}
				} else {
					dict[col] = value.String
				}
			} else {
				dict[col] = nil
			}
		}
		res = append(res, dict)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (h *Handler) GetTablesList(w http.ResponseWriter, r *http.Request) {
	// read table list
	query := "SHOW TABLES"

	rows, err := h.DB.Query(query)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "")
		return
	}

	tables, err := ReadTableNullString(h.DB, rows, nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "")
		return
	}
	rows.Close()

	//read schema for every column
	for _, row := range tables {
		for _, table := range row {
			table_schema := &Table{Fields: map[string]*FieldStruct{}}
			h.Schema.Tables[table.(string)] = table_schema
			query = fmt.Sprintf(
				"SHOW COLUMNS FROM %s", table.(string))
			rows, err := h.DB.Query(query)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, "")
				return
			}
			fields, _ := ReadTableNullString(h.DB, rows, nil)
			rows.Close()
			for _, f := range fields {
				field := &FieldStruct{Struct: f}
				table_schema.Fields[f["Field"].(string)] = field
				if field.Struct["Key"] == "PRI" {
					h.Schema.Tables[table.(string)].FieldId = f["Field"].(string)
				}
			}
		}
	}

	var tables_str []string
	for key, _ := range h.Schema.Tables {
		tables_str = append(tables_str, key)
	}
	sort.Strings(tables_str)

	//return JSON with data
	result := map[string]interface{}{
		"response": map[string]interface{}{
			"tables": tables_str,
		},
	}
	result_json, _ := json.Marshal(result)
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(result_json))
}

func UpdateLimitOffset(limit_str string, offset_str string, table_len int) (int, int) {
	var offset, limit int
	if offset_str == "" {
		offset = 0
	} else {
		val, e := strconv.Atoi(offset_str)
		if e != nil {
			offset = 0
		} else {
			offset = val
		}
	}

	if limit_str == "" {
		limit = 5
	} else {
		val, e := strconv.Atoi(limit_str)
		if e != nil {
			limit = 5
		} else {
			limit = val
		}
	}

	if table_len < limit {
		limit = table_len
	}

	if offset > table_len-limit {
		offset = table_len - limit
	}
	return limit, offset
}

func (h *Handler) GetTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	table := vars["table"]
	condition_id_bool := false
	id := 0
	if _, ok := vars["id"]; ok {
		condition_id_bool = true
		id, _ = strconv.Atoi(vars["id"])
	}

	offset_str := r.URL.Query().Get("offset")
	limit_str := r.URL.Query().Get("limit")

	if _, ok := h.Schema.Tables[table]; !ok {
		result := Result{}
		result["error"] = "unknown table"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}
	var rows *sql.Rows
	var query_str string
	if condition_id_bool {
		id_field := h.Schema.Tables[table].FieldId
		query_str = fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", table, id_field)
		rows, _ = h.DB.Query(query_str, id)

	} else {
		query_str = fmt.Sprintf("SELECT * FROM  %s", table)
		rows, _ = h.DB.Query(query_str)
	}
	content, err := ReadTableNullString(h.DB, rows, h.Schema.Tables[table].Fields)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "")
		return
	}
	rows.Close()

	var result map[string]interface{}
	if condition_id_bool {
		if len(content) == 0 {
			result := Result{}
			result["error"] = "record not found"
			result_json, _ := json.Marshal(result)
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, string(result_json))
			return
		}
		result = map[string]interface{}{
			"response": map[string]interface{}{
				"record": content[0],
			},
		}
	} else {
		limit, offset := UpdateLimitOffset(limit_str, offset_str, len(content))
		content = content[offset : offset+limit]
		result = map[string]interface{}{
			"response": map[string]interface{}{
				"records": content,
			},
		}
	}
	result_json, _ := json.Marshal(result)
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(result_json))
}

func (h *Handler) AddRow(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	table := vars["table"]

	b, _ := io.ReadAll(r.Body)
	var parameters map[string]interface{}
	_ = json.Unmarshal(b, &parameters)

	if _, ok := h.Schema.Tables[table]; !ok {
		result := Result{}
		result["error"] = "unknown table"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}

	var columns []string
	var values []interface{}
	var question_marks []string
	for col, val := range parameters {
		if _, ok := h.Schema.Tables[table].Fields[col]; ok {
			col_type := h.Schema.Tables[table].Fields[col].Struct["Type"]
			if h.Schema.Tables[table].Fields[col].Struct["Extra"] != "auto_increment" {
				switch x := val.(type) {
				case string:
					if col_type == "int" || col_type == "float" {
						result := Result{}
						result["error"] = fmt.Sprintf("field %s have invalid type", col)
						result_json, _ := json.Marshal(result)
						w.WriteHeader(http.StatusBadRequest)
						io.WriteString(w, string(result_json))
						return
					}
					columns = append(columns, col)
					values = append(values, val)
					question_marks = append(question_marks, "?")
				case float64:
					if col_type != "int" && col_type != "float" {
						result := Result{}
						result["error"] = fmt.Sprintf("field %s have invalid type", col)
						result_json, _ := json.Marshal(result)
						w.WriteHeader(http.StatusBadRequest)
						io.WriteString(w, string(result_json))
						return
					}
					columns = append(columns, col)
					values = append(values, val)
					question_marks = append(question_marks, "?")
				case nil:
					if h.Schema.Tables[table].Fields[col].Struct["Null"] != "YES" {
						result := Result{}
						result["error"] = fmt.Sprintf("field %s have invalid type", col)
						result_json, _ := json.Marshal(result)
						w.WriteHeader(http.StatusBadRequest)
						io.WriteString(w, string(result_json))
						return
					}
					columns = append(columns, col)
					values = append(values, sql.NullString{})
					question_marks = append(question_marks, "?")
				default:
					fmt.Println(x)
				}
			}
		}
	}

	for field, _ := range h.Schema.Tables[table].Fields {
		if _, ok := parameters[field]; ok {
			continue
		}
		if h.Schema.Tables[table].Fields[field].Struct["Null"] == "NO" {
			if h.Schema.Tables[table].Fields[field].Struct["Type"] == "int" {
				columns = append(columns, field)
				values = append(values, 0)
				question_marks = append(question_marks, "?")
			} else {
				columns = append(columns, field)
				values = append(values, "")
				question_marks = append(question_marks, "?")
			}
		}
	}

	column_list_str := strings.Join(columns, ", ")
	question_marks_str := strings.Join(question_marks, ", ")
	query_str := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, column_list_str, question_marks_str)
	sql_result, err := h.DB.Exec(query_str, values...)
	if err != nil {
		result := Result{}
		result["error"] = err
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}
	lastID, err := sql_result.LastInsertId()
	if err != nil {
		result := Result{}
		result["error"] = err
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}

	id_field := h.Schema.Tables[table].FieldId

	result := map[string]interface{}{
		"response": map[string]interface{}{
			id_field: lastID,
		},
	}

	result_json, _ := json.Marshal(result)
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(result_json))
}

func (h *Handler) UpdateRowById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	table := vars["table"]

	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		result := Result{}
		result["error"] = "incorrect id"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, string(result_json))
		return
	}

	b, _ := io.ReadAll(r.Body)
	var parameters map[string]interface{}
	_ = json.Unmarshal(b, &parameters)

	if _, ok := h.Schema.Tables[table]; !ok {
		result := Result{}
		result["error"] = "unknown table"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}

	var update_set []string
	var values []interface{}
	for col, val := range parameters {
		col_type := h.Schema.Tables[table].Fields[col].Struct["Type"]

		if h.Schema.Tables[table].Fields[col].Struct["Key"] != "PRI" {
			switch x := val.(type) {
			case string:
				if col_type == "int" || col_type == "float" {
					result := Result{}
					result["error"] = fmt.Sprintf("field %s have invalid type", col)
					result_json, _ := json.Marshal(result)
					w.WriteHeader(http.StatusBadRequest)
					io.WriteString(w, string(result_json))
					return
				}
				update_set = append(update_set, fmt.Sprintf("%s = ?", col))
				values = append(values, val)
			case float64:
				if col_type != "int" && col_type != "float" {
					result := Result{}
					result["error"] = fmt.Sprintf("field %s have invalid type", col)
					result_json, _ := json.Marshal(result)
					w.WriteHeader(http.StatusBadRequest)
					io.WriteString(w, string(result_json))
					return
				}
				update_set = append(update_set, fmt.Sprintf("%s = ?", col))
				values = append(values, val)
			case nil:
				if h.Schema.Tables[table].Fields[col].Struct["Null"] != "YES" {
					result := Result{}
					result["error"] = fmt.Sprintf("field %s have invalid type", col)
					result_json, _ := json.Marshal(result)
					w.WriteHeader(http.StatusBadRequest)
					io.WriteString(w, string(result_json))
					return
				}
				update_set = append(update_set, fmt.Sprintf("%s = ?", col))
				values = append(values, sql.NullString{})
			default:
				fmt.Println(x)
			}
		} else {
			result := Result{}
			result["error"] = fmt.Sprintf("field %s have invalid type", h.Schema.Tables[table].FieldId)
			result_json, _ := json.Marshal(result)
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, string(result_json))
			return
		}
	}

	update_set_str := strings.Join(update_set, ", ")
	id_field := h.Schema.Tables[table].FieldId
	query_str := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %d", table, update_set_str, id_field, id)
	sql_result, err := h.DB.Exec(query_str, values...)
	if err != nil {
		result := Result{}
		result["error"] = err
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}
	affected, err := sql_result.RowsAffected()
	if err != nil {
		result := Result{}
		result["error"] = err
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
	}

	result := map[string]interface{}{
		"response": map[string]interface{}{
			"updated": affected,
		},
	}

	result_json, _ := json.Marshal(result)
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(result_json))
}

func (h *Handler) DeleteRowById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	table := vars["table"]

	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		result := Result{}
		result["error"] = "incorrect id"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, string(result_json))
		return
	}

	if _, ok := h.Schema.Tables[table]; !ok {
		result := Result{}
		result["error"] = "unknown table"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}
	id_field := h.Schema.Tables[table].FieldId
	query_str := fmt.Sprintf("DELETE FROM  %s WHERE %s = %d", table, id_field, id)
	sql_result, err := h.DB.Exec(query_str)
	if err != nil {
		result := Result{}
		result["error"] = "Exec error"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
		return
	}
	affected, err := sql_result.RowsAffected()
	if err != nil {
		result := Result{}
		result["error"] = "RowsAffected error"
		result_json, _ := json.Marshal(result)
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, string(result_json))
	}
	result := map[string]interface{}{
		"response": map[string]interface{}{
			"deleted": affected,
		},
	}

	result_json, _ := json.Marshal(result)
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(result_json))
}

func NewDbExplorer(db *sql.DB) (*mux.Router, error) {

	db.SetMaxOpenConns(10)

	schema := &Schema{Tables: map[string]*Table{}}

	handlers := &Handler{
		DB:     db,
		Schema: schema,
	}

	r := mux.NewRouter()
	r.HandleFunc("/", handlers.GetTablesList).Methods()
	r.HandleFunc("/", handlers.GetTablesList).Methods(http.MethodGet)
	r.HandleFunc("/{table}", handlers.GetTable).Methods()
	r.HandleFunc("/{table}", handlers.GetTable).Methods(http.MethodGet)
	r.HandleFunc("/{table}/{id}", handlers.GetTable).Methods(http.MethodGet)
	r.HandleFunc("/{table}/{id}", handlers.UpdateRowById).Methods(http.MethodPost)
	r.HandleFunc("/{table}/{id}", handlers.UpdateRowById).Methods(http.MethodPut)
	r.HandleFunc("/{table}/", handlers.AddRow).Methods(http.MethodPost)
	r.HandleFunc("/{table}/", handlers.AddRow).Methods(http.MethodPut)
	r.HandleFunc("/{table}/{id}", handlers.DeleteRowById).Methods(http.MethodDelete)

	return r, nil
}
