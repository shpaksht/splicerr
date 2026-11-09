#!/bin/sh
set -eu

cd "$(dirname "$0")"
output="${1:-$PWD/dist}"
mkdir -p "$output"
output="$(cd "$output" && pwd)"
bundle="$output/Splicerr Native.app"

cargo build --locked --release
version="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml)"
target_dir="${CARGO_TARGET_DIR:-target}"
mkdir -p "$bundle/Contents/MacOS" "$bundle/Contents/Resources"
install -m 755 "$target_dir/release/splicerr-native" "$bundle/Contents/MacOS/splicerr-native"
install -m 644 ../src-tauri/icons/icon.icns "$bundle/Contents/Resources/AppIcon.icns"
cat > "$bundle/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDisplayName</key><string>Splicerr Native</string>
    <key>CFBundleName</key><string>Splicerr Native</string>
    <key>CFBundleIdentifier</key><string>com.splicerr.native</string>
    <key>CFBundleExecutable</key><string>splicerr-native</string>
    <key>CFBundleIconFile</key><string>AppIcon</string>
    <key>CFBundlePackageType</key><string>APPL</string>
    <key>CFBundleShortVersionString</key><string>$version</string>
    <key>CFBundleVersion</key><string>$version</string>
    <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST
codesign --force --sign - "$bundle"
codesign --verify --strict "$bundle"
printf '%s\n' "$bundle"
