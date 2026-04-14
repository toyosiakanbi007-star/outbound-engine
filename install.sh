#!/bin/bash
# install.sh — Run this from ~/outbound-engine after unzipping
# Usage: cd ~/outbound-engine && bash install.sh

set -e

echo "=== Outbound Engine: Onboarding AI Integration ==="

# 1. Run migration
echo "[1/4] Running database migration..."
source backend/.env 2>/dev/null || true
if [ -n "$DATABASE_URL" ]; then
    psql "$DATABASE_URL" -f migrations/0040_client_onboarding.sql
    echo "  ✓ Migration applied"
else
    echo "  ⚠ DATABASE_URL not set — run manually: psql \$DATABASE_URL -f migrations/0040_client_onboarding.sql"
fi

# 2. Add Cargo.toml dependencies if missing
echo "[2/4] Checking Cargo.toml dependencies..."
cd backend

if ! grep -q 'urlencoding' Cargo.toml 2>/dev/null; then
    # Add before the last empty line or at end of [dependencies]
    sed -i '/^\[dependencies\]/,/^\[/{
        /^$/i urlencoding = "2"
        /^$/i url = "2"
    }' Cargo.toml 2>/dev/null || {
        # Fallback: just append to file before any [[bin]] or [profile] section
        echo 'urlencoding = "2"' >> Cargo.toml
        echo 'url = "2"' >> Cargo.toml
    }
    echo "  ✓ Added urlencoding + url to Cargo.toml"
else
    echo "  ✓ urlencoding already in Cargo.toml"
fi

# Check lazy_static (should already exist)
if ! grep -q 'lazy_static' Cargo.toml 2>/dev/null; then
    sed -i '/^\[dependencies\]/a lazy_static = "1"' Cargo.toml 2>/dev/null || echo 'lazy_static = "1"' >> Cargo.toml
    echo "  ✓ Added lazy_static to Cargo.toml"
fi

cd ..

# 3. Add Azure OpenAI env vars if missing
echo "[3/4] Checking .env for Azure OpenAI vars..."
if ! grep -q 'AZURE_OPENAI_ENDPOINT' backend/.env 2>/dev/null; then
    cat >> backend/.env << 'ENVEOF'

# ==============================================================================
# Azure OpenAI (for Client Onboarding AI — direct LLM calls from Rust)
# ==============================================================================
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com
AZURE_OPENAI_API_KEY=your-api-key-here
AZURE_OPENAI_DEPLOYMENT=gpt-4o
AZURE_OPENAI_API_VERSION=2024-08-01-preview
ENVEOF
    echo "  ✓ Added Azure OpenAI vars to .env (edit with real values!)"
else
    echo "  ✓ Azure OpenAI vars already in .env"
fi

# 4. Build
echo "[4/4] Building backend..."
cd backend
cargo build 2>&1 | tail -5
echo ""
echo "=== Done! ==="
echo ""
echo "Next steps:"
echo "  1. Edit backend/.env — set AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_DEPLOYMENT"
echo "  2. Restart server:  cd backend && MODE=server cargo run"
echo "  3. Restart worker:  cd backend && MODE=worker+prequal_listener cargo run"
echo "  4. Restart frontend: cd frontend && npm run dev"
echo "  5. Open http://localhost:3001/onboarding"
