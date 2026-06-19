package controller

import (
	"os"
	"testing"

	"sigs.k8s.io/yaml"

	integronv1alpha1 "github.com/integronlabs/integron-k3s/api/v1alpha1"
)

const sampleSpec = `openapi: 3.0.3
info:
  title: Dog Facts
  version: 1.0.0
servers:
  - url: http://localhost:8080
paths:
  /facts:
    get:
      operationId: getDogFact
      responses:
        '200': { description: ok }
        '500': { description: err }
      x-integron-steps:
        - name: dogFacts
          type: http
          url: 'https://dogapi.dog/api/v2/facts?limit=$.request.amount'
          method: GET
          responses:
            '200': { output: { response: $.body, status: $.status }, next: arrayTransform }
        - name: arrayTransform
          type: transformarray
          input: $.dogFacts.response.data
          output: { fact: $.attributes.body, id: $.id }
          next: responseMarshal
        - name: responseMarshal
          type: transformobject
          output: { body: { data: $.arrayTransform } }
          next: ""
        - name: error
          type: error
          next: ""
`

func TestNormalizeBasePath(t *testing.T) {
	cases := map[string]string{
		"":          "",
		"/":         "",
		"dogfacts":  "/dogfacts",
		"/dogfacts": "/dogfacts",
		"/dog/":     "/dog",
		"  /x  ":    "/x",
	}
	for in, want := range cases {
		if got := normalizeBasePath(in); got != want {
			t.Errorf("normalizeBasePath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEffectiveBasePath(t *testing.T) {
	mk := func(name, bp string) *integronv1alpha1.IntegronAPI {
		a := &integronv1alpha1.IntegronAPI{}
		a.Name = name
		a.Spec.BasePath = bp
		return a
	}
	cases := []struct{ name, bp, want string }{
		{"dogfacts", "", "/dogfacts"},        // default to /<name>
		{"dogfacts", "/", "/dogfacts"},       // "/" is root => default
		{"dogfacts", "/v1/dogs", "/v1/dogs"}, // explicit wins
		{"echo", "echo", "/echo"},            // normalized
	}
	for _, c := range cases {
		if got := effectiveBasePath(mk(c.name, c.bp)); got != c.want {
			t.Errorf("effectiveBasePath(name=%q, basePath=%q) = %q, want %q", c.name, c.bp, got, c.want)
		}
	}
}

func TestWithBasePath(t *testing.T) {
	out, err := withBasePath(sampleSpec, "/dogfacts")
	if err != nil {
		t.Fatalf("withBasePath: %v", err)
	}

	// Optional: dump for live engine verification.
	if p := os.Getenv("INTEGRON_DUMP"); p != "" {
		if err := os.WriteFile(p, []byte(out), 0o644); err != nil {
			t.Fatalf("dump: %v", err)
		}
	}

	var doc map[string]interface{}
	if err := yaml.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("rewritten spec does not parse: %v", err)
	}

	// servers must be exactly the injected relative base path.
	servers, ok := doc["servers"].([]interface{})
	if !ok || len(servers) != 1 {
		t.Fatalf("servers = %v, want one entry", doc["servers"])
	}
	if url := servers[0].(map[string]interface{})["url"]; url != "/dogfacts" {
		t.Fatalf("servers[0].url = %v, want /dogfacts", url)
	}

	// The first step must still be dogFacts — the engine uses steps[0] as the
	// pipeline entry point, so array order must survive the round-trip.
	steps := doc["paths"].(map[string]interface{})["/facts"].(map[string]interface{})["get"].(map[string]interface{})["x-integron-steps"].([]interface{})
	if len(steps) != 4 {
		t.Fatalf("got %d steps, want 4", len(steps))
	}
	if name := steps[0].(map[string]interface{})["name"]; name != "dogFacts" {
		t.Fatalf("steps[0].name = %v, want dogFacts (order not preserved)", name)
	}
}
