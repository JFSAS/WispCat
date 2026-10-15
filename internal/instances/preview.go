package instances

import "path/filepath"

// Preview resolves an instance's namespace without reading account state or
// creating directories. Configuration checks use it to find saved overrides.
func Preview(root, key string) (Profile, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return Profile{}, err
	}
	return profile(absolute, key)
}
