package media

import (
	"bytes"
	"encoding/xml"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	maxDepth      = 16
	maxTokens     = 4096
	maxAttributes = 512
)

type element struct {
	name     string
	attrs    map[string]string
	children []*element
	text     strings.Builder
}

func parseXML(content []byte) (*element, error) {
	if len(content) > MaxXMLBytes {
		return nil, ErrTooLarge
	}
	if len(content) == 0 || !utf8.Valid(content) {
		return nil, ErrMalformed
	}
	decoder := xml.NewDecoder(bytes.NewReader(content))
	decoder.Strict = true
	var root *element
	var stack []*element
	attributes := 0
	for tokens := 0; ; tokens++ {
		if tokens >= maxTokens {
			return nil, ErrTooLarge
		}
		token, err := decoder.Token()
		if err == io.EOF {
			if root == nil || len(stack) != 0 {
				return nil, ErrMalformed
			}
			return root, nil
		}
		if err != nil {
			// XML errors may embed private source text. Do not return the decoder
			// error or attach it to a public error chain.
			return nil, ErrMalformed
		}
		switch t := token.(type) {
		case xml.StartElement:
			if len(stack) >= maxDepth {
				return nil, ErrTooLarge
			}
			if t.Name.Space != "" {
				return nil, ErrMalformed
			}
			attributes += len(t.Attr)
			if attributes > maxAttributes {
				return nil, ErrTooLarge
			}
			node := &element{name: t.Name.Local, attrs: map[string]string{}}
			for _, attr := range t.Attr {
				if attr.Name.Space != "" {
					return nil, ErrMalformed
				}
				if _, duplicate := node.attrs[attr.Name.Local]; duplicate {
					return nil, ErrMalformed
				}
				node.attrs[attr.Name.Local] = attr.Value
			}
			if len(stack) == 0 {
				if root != nil {
					return nil, ErrMalformed
				}
				root = node
			} else {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, node)
			}
			stack = append(stack, node)
		case xml.EndElement:
			if len(stack) == 0 {
				return nil, ErrMalformed
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) == 0 {
				if strings.TrimSpace(string(t)) != "" {
					return nil, ErrMalformed
				}
			} else {
				stack[len(stack)-1].text.Write(t)
			}
		case xml.Directive:
			// encoding/xml never fetches external entities. Reject DTD and
			// other directives as well, instead of accepting unused declarations.
			return nil, ErrMalformed
		case xml.ProcInst:
			if t.Target != "xml" || root != nil {
				return nil, ErrMalformed
			}
		}
	}
}

func (e *element) child(name string) (*element, error) {
	var found *element
	for _, child := range e.children {
		if child.name != name {
			continue
		}
		if found != nil {
			return nil, ErrMalformed
		}
		found = child
	}
	return found, nil
}

func (e *element) value(name string) (string, error) {
	child, err := e.child(name)
	if err != nil || child == nil {
		return "", err
	}
	if len(child.children) != 0 {
		return "", ErrMalformed
	}
	return strings.TrimSpace(child.text.String()), nil
}
