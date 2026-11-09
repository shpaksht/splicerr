# Splicerr Native

A native desktop client for browsing and auditioning the Splice catalog. The interface, networking, preview decoding and WAV export run in Rust. The macOS bundle uses its own app identifier and can run alongside the original Splicerr.

## Build and run

Use a current stable Rust toolchain. Run these commands from the repository root on macOS.

```sh
cargo run --release --locked --manifest-path native/Cargo.toml
native/package-macos.sh
```

The second command creates a locally signed `native/dist/Splicerr Native.app` for the Mac architecture on which it was built. Pass an absolute directory to put the bundle somewhere else.

- Type in the search field or press Enter to submit immediately. Command+F returns focus to search. Filter by tags, pack, key, mode, BPM and sound type.
- Play a preview with the triangle, the filename or Space when the search field is unfocused. Click the waveform to audition from that point.
- Press **WAV** to prepare a 16-bit WAV from the available audio preview. When **Drag** appears, drag the file to a DAW or click to reveal it in Finder. Use Settings to choose the export folder.
- Like a sample or add it to a collection from the row menu. On first launch, the app reads compatible settings and saved sample records from the old macOS app when available.

WAV exports contain preview audio. The native version focuses on searching and auditioning. Pitch shifting from some desktop builds is outside this version.

## Responsiveness

The original frontend adds rows as it scrolls and keeps each loaded row and its SVG waveform mounted. Its waveform component also starts fetching when mounted. On a Mac running Splicerr 1.0.13, a profile during a freeze showed a WebKit process at roughly one core with its main thread occupied by DOM mutations.

This client fetches 50 catalog entries per page and builds only the visible row widgets. It cancels pending search and suggestion requests when filters change, ignores stale responses, and deduplicates page results by sample ID. Waveforms and pack art load for the visible window through up to six concurrent requests. The media cache holds up to 192 entries.

## Verify

```sh
cargo test --locked --manifest-path native/Cargo.toml
cargo run --locked --release --manifest-path native/Cargo.toml -- --check-catalog
```

The tests cover stale search responses, pagination deduplication, the legacy preview envelope and row virtualization over 115,323 synthetic entries. The live check searches for `drum and bass drums` with Loops, checks two pages and autocomplete, downloads a waveform and cover, decodes one preview and validates a temporary WAV. Run with `--demo` for a local UI fixture.
