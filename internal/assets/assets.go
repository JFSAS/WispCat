// Package assets embeds version-bound public parameters and the shared app
// storage constant. No account credentials or compiled artifacts are embedded.
package assets

import "embed"

//go:embed production-profile.json production-roots.pem protection.json
var Files embed.FS
