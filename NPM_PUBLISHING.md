# NPM Publishing Guide for f9-db

This guide covers how to publish the f9-db package to the npm registry.

## Prerequisites

Before publishing, ensure you have:
- Node.js and npm installed (you have this already)
- An npm account (create one at https://www.npmjs.com/signup if needed)
- Permissions to publish under the package name `f9-db`

## Package Configuration Status

✅ **Your package is already well-configured!** Here's what's in place:

- **package.json**: Contains all required fields
  - `name`: f9-db
  - `version`: 0.9.1
  - `description`: Pure Javascript database
  - `main`: ./dist/index.js (entry point)
  - `types`: ./dist/index.d.ts (TypeScript definitions)
  - `files`: ["dist"] (only distributes compiled code)
  - `license`: MIT
  - `repository`: GitHub URL configured
  - `keywords`: Includes searchable terms

- **Build scripts**: Added `prepublishOnly` and `prepare` hooks
  - `prepublishOnly`: Runs before publishing (clean → build → test)
  - `prepare`: Runs after install (ensures dist/ is built)

- **.npmignore**: Created to exclude unnecessary files (src/, tests/, configs)

- **TypeScript**: Properly configured with declaration files

## Step-by-Step Publishing Process

### 1. Create an npm Account (First Time Only)

If you don't have an npm account:

```bash
npm adduser
```

Or create one at: https://www.npmjs.com/signup

### 2. Login to npm

```bash
npm login
```

You'll be prompted for:
- Username
- Password
- Email (this is public)
- One-time password (if you have 2FA enabled)

### 3. Verify Your Login

```bash
npm whoami
```

This should display your npm username.

### 4. Check Package Name Availability

Before publishing, verify the name isn't taken:

```bash
npm view f9-db
```

- If you see "404 Not Found" or "npm ERR! code E404", the name is available
- If package details appear, the name is taken (you'd need to choose a different name or get permissions)

### 5. Validate Package Contents

Double-check what will be published:

```bash
npm pack --dry-run
```

This shows exactly what files will be included. You should see:
- dist/ directory with compiled JavaScript and TypeScript declarations
- README.md
- LICENSE
- package.json

### 6. Run Tests Before Publishing

Ensure everything works:

```bash
npm test
```

### 7. Update Version (if needed)

Before each release, update the version using semantic versioning:

```bash
# For bug fixes (0.9.1 → 0.9.2)
npm version patch

# For new features (0.9.1 → 0.10.0)
npm version minor

# For breaking changes (0.9.1 → 1.0.0)
npm version major
```

This automatically updates package.json and creates a git tag.

### 8. Publish to npm

For the first publication:

```bash
npm publish
```

For scoped packages (e.g., @foundry-9/f9-db), use:

```bash
npm publish --access public
```

The `prepublishOnly` script will automatically:
1. Clean the dist/ directory
2. Rebuild from source
3. Run tests
4. Then publish if all succeeds

### 9. Verify Publication

After publishing:

```bash
npm view f9-db
```

Or visit: https://www.npmjs.com/package/f9-db

### 10. Test Installation

Try installing your package in a test project:

```bash
mkdir test-f9-db
cd test-f9-db
npm init -y
npm install f9-db
```

## Updating the Package

For subsequent releases:

1. Make your changes to the source code
2. Update tests
3. Run `npm version patch|minor|major`
4. Push to git: `git push && git push --tags`
5. Publish: `npm publish`

## Package Scopes (Optional)

If you want to publish under your organization namespace (@foundry-9/f9-db):

1. Update package.json name: `"name": "@foundry-9/f9-db"`
2. Publish with: `npm publish --access public`

## Unpublishing (Use with Caution)

If you need to unpublish within 72 hours:

```bash
npm unpublish f9-db@0.9.1
```

⚠️ **Warning**: Unpublishing is discouraged and has restrictions. Use deprecation instead:

```bash
npm deprecate f9-db@0.9.1 "Use version X.X.X instead"
```

## Best Practices

1. **Always test before publishing**: Run `npm test`
2. **Use semantic versioning**: Follow semver.org
3. **Keep README updated**: It appears on the npm package page
4. **Tag releases in git**: `npm version` does this automatically
5. **Use .npmignore**: Only publish what's necessary
6. **Enable 2FA**: Protect your npm account with two-factor authentication

## Troubleshooting

### "You do not have permission to publish"
- The package name might be taken
- Try a scoped package: @your-username/f9-db
- Or choose a different name

### "Package name too similar to existing package"
- Choose a more unique name
- Consider a scoped package

### "prepublishOnly script failed"
- Check that tests pass: `npm test`
- Verify build succeeds: `npm run build`
- Check for lint errors: `npm run lint`

## Current Package Status

Your package (v0.9.1) is configured correctly and ready to publish!

**Files that will be published** (73.7 kB gzipped):
- dist/ (compiled JS + TypeScript definitions + source maps)
- README.md
- LICENSE
- package.json

**Total size**: 388.3 kB unpacked

## Quick Reference Commands

```bash
# Login
npm login

# Verify login
npm whoami

# Check what will be published
npm pack --dry-run

# Update version and publish
npm version patch
npm publish

# View published package
npm view f9-db
```

---

**Ready to publish?** Just run `npm login` and then `npm publish`!
