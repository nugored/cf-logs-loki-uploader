package models

// LogEntry defines the structure for a single W3C log record.
type LogEntry map[string]string

// W3CLog represents the entire log file, including the header fields.
type W3CLog struct {
	HeaderFields []string
	Records      []LogEntry
}
