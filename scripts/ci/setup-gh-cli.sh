#!/bin/bash
# setup-gh-cli.sh - Install and configure GitHub CLI for GeoBriX CI management

set -e

echo "🔧 Setting up GitHub CLI for GeoBriX..."

# Check if gh is already installed
if command -v gh &> /dev/null; then
    echo "✅ GitHub CLI is already installed: $(gh --version)"
else
    echo "📦 Installing GitHub CLI..."
    
    # Detect OS and install accordingly
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            echo "Using Homebrew to install gh..."
            brew install gh
        else
            echo "❌ Error: Homebrew not found. Please install from https://brew.sh/"
            echo "Or install GitHub CLI manually: https://cli.github.com/manual/installation"
            exit 1
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        if command -v apt-get &> /dev/null; then
            echo "Using apt to install gh..."
            sudo apt-get update
            sudo apt-get install -y gh
        elif command -v yum &> /dev/null; then
            echo "Using yum to install gh..."
            sudo yum install -y gh
        else
            echo "❌ Error: Package manager not found."
            echo "Please install GitHub CLI manually: https://cli.github.com/manual/installation"
            exit 1
        fi
    else
        echo "❌ Error: Unsupported OS: $OSTYPE"
        echo "Please install GitHub CLI manually: https://cli.github.com/manual/installation"
        exit 1
    fi
fi

# Check if gh is authenticated
if gh auth status &> /dev/null; then
    echo "✅ GitHub CLI is already authenticated"
else
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔐 GitHub CLI Authentication Required"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "You need to authenticate with GitHub. Choose a method:"
    echo ""
    echo "  1) Browser (recommended) - You'll see a code like: ABCD-1234"
    echo "  2) Token - Generate at: https://github.com/settings/tokens"
    echo ""
    echo "⚠️  IMPORTANT: Watch for the one-time code in the output!"
    echo "    It will look like: 'First copy your one-time code: XXXX-XXXX'"
    echo ""
    read -p "Press Enter to start authentication..." 
    echo ""
    
    # Try to authenticate
    if gh auth login; then
        echo ""
        echo "✅ Authentication successful!"
    else
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "⚠️  Authentication Issue Detected"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "If you didn't see the one-time code, try this:"
        echo ""
        echo "  1. Open a NEW terminal window"
        echo "  2. Run: gh auth login"
        echo "  3. Look for: 'First copy your one-time code: XXXX-XXXX'"
        echo "  4. Enter that code on the GitHub page"
        echo ""
        echo "Or use token authentication:"
        echo "  1. Visit: https://github.com/settings/tokens"
        echo "  2. Click 'Generate new token (classic)'"
        echo "  3. Select scopes: 'repo', 'workflow', and 'read:org'"
        echo "  4. Run: gh auth login"
        echo "  5. Choose 'Paste an authentication token'"
        echo ""
        echo "After authenticating, re-run this script."
        echo ""
        exit 1
    fi
fi

# Verify installation and auth
echo ""
echo "📊 GitHub CLI Status:"
gh --version
echo ""
gh auth status

echo ""
echo "✅ GitHub CLI setup complete!"
echo ""
echo "Next steps:"
echo "  1. Run: ./scripts/ci/trigger-remote-tests.sh"
echo "  2. Or: ./scripts/ci/check-ci-status.sh"
