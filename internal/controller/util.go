package controller

import "regexp"

// camelCaseSplit is used by friendlyName to split CamelCase community map IDs
// like "MyCustomMap" into "My Custom Map" for session-name display.
var camelCaseSplit = regexp.MustCompile(`([a-z0-9])([A-Z])`)
