package tasks

import (
	"fmt"
	"testing"
)

func TestFormatLocateValueJSONBytes(t *testing.T) {
	jsonBytes := []byte(`["357 F.2d 756","1966 U.S. App. LEXIS 7450"]`)
	want := fmt.Sprintf(`%s (json size: %d bytes)`, string(jsonBytes), len(jsonBytes))
	if got := formatLocateValue(jsonBytes); got != want {
		t.Fatalf("formatLocateValue() = %v, want %s", got, want)
	}
}

func TestFormatLocateValueNonJSONBytes(t *testing.T) {
	rawBytes := []byte{0x01, 0x02}
	got, ok := formatLocateValue(rawBytes).([]byte)
	if !ok {
		t.Fatalf("formatLocateValue() type = %T, want []byte", got)
	}
	if string(got) != string(rawBytes) {
		t.Fatalf("formatLocateValue() = %v, want %v", got, rawBytes)
	}
}
