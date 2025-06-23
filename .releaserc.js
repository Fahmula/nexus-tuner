// .releaserc.js

const config = {
  branches: ['main'], // Default branch
  plugins: [
    '@semantic-release/commit-analyzer',
    '@semantic-release/release-notes-generator',
    ['@semantic-release/changelog', {
      changelogFile: 'CHANGELOG.md',
    }],
    // The github plugin is necessary to create the GitHub release
    '@semantic-release/github',
    ['@semantic-release/git', {
      assets: ['CHANGELOG.md'],
      message: 'chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}',
    }],
  ],
};

// =================================================================================
// DYNAMIC LOGIC: This section reads environment variables from the workflow
// and modifies the configuration object before running.
// =================================================================================

// Get inputs from the GitHub Actions environment
const isPrerelease = process.env.IS_PRERELEASE === 'true';
const branchName = process.env.GITHUB_REF_NAME;
const prereleaseChannel = process.env.PRERELEASE_CHANNEL;
const forceReleaseType = process.env.FORCE_RELEASE_TYPE;

// 1. Handle pre-releases
if (isPrerelease) {
  console.log(`Configuring for pre-release on channel: ${prereleaseChannel}`);
  // Override the branches configuration for a pre-release
  config.branches = [
    { name: branchName, prerelease: prereleaseChannel }
  ];
}

// 2. Handle forcing a release type (patch, minor, major)
if (forceReleaseType) {
  console.log(`Forcing a release of type: ${forceReleaseType}`);
  // This is a workaround to force a release of a specific type.
  // We add a 'releaseRules' array to the commit-analyzer that will
  // trigger a release of the desired type for any commit.
  config.plugins[0] = [
    '@semantic-release/commit-analyzer', {
      releaseRules: [
        { release: forceReleaseType }
      ]
    }
  ];
}

// Export the final, modified configuration
module.exports = config;