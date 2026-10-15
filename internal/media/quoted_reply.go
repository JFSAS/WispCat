package media

// QuotedReplyText extracts only the new reply's text from appmsg type57.
// The referenced message may contain private media XML; never expose it.
func QuotedReplyText(content []byte) (string, error) {
	root, err := parseXML(content)
	if err != nil {
		return "", err
	}
	if root.name != "msg" {
		return "", ErrUnsupported
	}
	app, err := root.child("appmsg")
	if err != nil || app == nil {
		return "", ErrUnsupported
	}
	typeText, err := app.value("type")
	if err != nil || typeText != "57" {
		return "", ErrUnsupported
	}
	title, err := app.value("title")
	if err != nil || title == "" {
		return "", ErrMalformed
	}
	if len(title) > MaxTextBytes {
		return "", ErrTooLarge
	}
	return title, nil
}
