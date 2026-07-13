## Change

Describe the user-visible behavior and affected public API.

## Linked issue

Closes #

## Verification

- [ ] `mise exec go@1.26.5 -- go build ./...`
- [ ] `mise exec go@1.26.5 -- go vet ./...`
- [ ] `mise exec go@1.26.5 -- go test -race -count=1 ./...`
- [ ] Compatibility with the latest release tag is evaluated.
- [ ] Security impact and dependency changes are evaluated.
- [ ] README, CHANGELOG, and examples are updated when public behavior changes.

## Release note

- [ ] No release note required.
- [ ] Added release note below.

<!-- Write the release note here. -->
