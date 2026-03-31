package api

import (
	"testing"
)

func TestParseAuthResource(t *testing.T) {
	tests := []struct {
		path     string
		wantType string
		wantLink string
	}{
		// Root
		{"/", "", ""},

		// Database collection endpoint
		{"/dbs", "dbs", ""},

		// Single database
		{"/dbs/mydb", "dbs", "dbs/mydb"},

		// Container collection endpoint
		{"/dbs/mydb/colls", "colls", "dbs/mydb"},

		// Single container
		{"/dbs/mydb/colls/mycoll", "colls", "dbs/mydb/colls/mycoll"},

		// Document collection endpoint
		{"/dbs/mydb/colls/mycoll/docs", "docs", "dbs/mydb/colls/mycoll"},

		// Single document
		{"/dbs/mydb/colls/mycoll/docs/mydoc", "docs", "dbs/mydb/colls/mycoll/docs/mydoc"},

		// Partition key ranges
		{"/dbs/mydb/colls/mycoll/pkranges", "pkranges", "dbs/mydb/colls/mycoll"},

		// RID-based paths
		{"/dbs/DPZtAA==/colls/DPZtAKyOj5g=/pkranges", "pkranges", "dbs/DPZtAA==/colls/DPZtAKyOj5g="},
		{"/dbs/DPZtAA==/colls/DPZtAKyOj5g=/docs", "docs", "dbs/DPZtAA==/colls/DPZtAKyOj5g="},
		{"/dbs/DPZtAA==/colls/DPZtAKyOj5g=/docs/DPZtAKyOj5gBAAAAAAAAAA==", "docs", "dbs/DPZtAA==/colls/DPZtAKyOj5g=/docs/DPZtAKyOj5gBAAAAAAAAAA=="},

		// Trailing slash handling
		{"/dbs/mydb/", "dbs", "dbs/mydb"},
		{"/dbs/mydb/colls/", "colls", "dbs/mydb"},

		// Attachments
		{"/dbs/rid1/colls/rid2/docs/rid3/attachments", "attachments", "dbs/rid1/colls/rid2/docs/rid3"},
		{"/dbs/rid1/colls/rid2/docs/rid3/attachments/rid4", "attachments", "dbs/rid1/colls/rid2/docs/rid3/attachments/rid4"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			gotType, gotLink := parseAuthResource(tt.path)
			if gotType != tt.wantType {
				t.Errorf("parseAuthResource(%q) resourceType = %q, want %q", tt.path, gotType, tt.wantType)
			}
			if gotLink != tt.wantLink {
				t.Errorf("parseAuthResource(%q) resourceLink = %q, want %q", tt.path, gotLink, tt.wantLink)
			}
		})
	}
}
