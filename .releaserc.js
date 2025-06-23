// .releaserc.js

console.log('--- .releaserc.js: STARTING CONFIGURATION ---');

// --- Base Configuration ---
// This is your standard setup for a release from the 'main' branch.
const config = {
  branches: ['main'],
  plugins: [
    '@semantic-release/commit-analyzer',
    '@semantic-release/release-notes-generator',
    ['@semantic-release/changelog', { changelogFile: 'CHANGELOG.md' }],
    '@semantic-release/github',
    ['@semantic-release/git', {
      assets: ['CHANGELOG.md'],
      message: 'chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}',
    }],
  ],
};

// --- Dynamic Overrides from Workflow ---
// Read environment variables passed from the GitHub Actions workflow.
const isPrerelease = process.env.IS_PRERELEASE === 'true';
const branchName = process.env.GITHUB_REF_NAME;
const prereleaseChannel = process.env.PRERELEASE_CHANNEL;
const forceReleaseType = process.env.FORCE_RELEASE_TYPE;

console.log(`- Detected Branch Name: [${branchName}]`);
console.log(`- Is Prerelease?: [${isPrerelease}]`);
console.log(`- Prerelease Channel: [${prereleaseChannel}]`);

// ** DYNAMIC BRANCH LOGIC **
// If the 'is_prerelease' input is true, we completely override the 'branches' config.
if (isPrerelease) {
  // We need to ensure both branchName and prereleaseChannel have values.
  if (branchName && prereleaseChannel) {
    console.log(`>>> Configuring for PRE-RELEASE on branch "${branchName}" with channel "${prereleaseChannel}".`);
    config.branches = [
      { name: branchName, prerelease: prereleaseChannel }
    ];
  } else {
    // This is a critical failure case. We log it clearly.
    console.error('!!! ERROR: isPrerelease is true, but branchName or prereleaseChannel is missing!');
    console.error(`!!! branchName: ${branchName}, prereleaseChannel: ${prereleaseChannel}`);
  }
} else {
    console.log('>>> Configuring for a REGULAR release. Default branches will be used unless the current branch is added.');
    // Optional: If you want to allow regular releases from feature branches, you could add logic here.
    // For now, it will correctly fail if you try a non-prerelease on a non-main branch.
}

// ** DYNAMIC FORCE-RELEASE LOGIC **
if (forceReleaseType) {
  console.log(`>>> Forcing a release of type: [${forceReleaseType}]`);
  config.plugins[0] = [
    '@semantic-release/commit-analyzer', {
      releaseRules: [
        { release: forceReleaseType }
      ]
    }
  ];
}

console.log('--- .releaserc.js: FINAL CONFIGURATION ---');
console.log(JSON.stringify(config, null, 2));
console.log('------------------------------------------');

module.exports = config;