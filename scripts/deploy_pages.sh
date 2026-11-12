#!/usr/bin/env bash
# Build the console for GitHub Pages (served from /downrange/) and push it to gh-pages.
# The hosted build runs the recorded-session replay and the direct live ISS mode;
# the local gateway mode only works when you run ./run.sh yourself.
set -euo pipefail
cd "$(dirname "$0")/../console"
ORIGIN="$(git remote get-url origin)"
PAGES_BASE=/downrange/ npm run build
cd dist
touch .nojekyll
rm -rf .git
git init -q -b gh-pages
git add -A
git -c user.name="Mohith Gajjela" -c user.email="mohithgajjela@gmail.com" commit -qm "Deploy console build"
git push -q -f "$ORIGIN" gh-pages
rm -rf .git
echo "pushed gh-pages"
