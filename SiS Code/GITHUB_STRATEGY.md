# GitHub Strategy for SiS Code

This document outlines the steps to commit the Streamlit in Snowflake (SiS) code to your GitHub repository.

## Steps to Commit and Push to GitHub

1. **Add the Files to Git**:
   ```bash
   git add "SiS Code"
   ```

2. **Commit the Changes with a Descriptive Message**:
   ```bash
   git commit -m "Add Streamlit in Snowflake application for Support Conversations Analysis"
   ```

3. **Push to GitHub**:
   ```bash
   git push origin main
   ```
   (Replace `main` with your branch name if different)

## Optional: Create a GitHub Release (For Version Control)

If you want to create a release for this version of the SiS application:

1. Go to your GitHub repository in a web browser
2. Click on "Releases" in the right sidebar
3. Click "Create a new release"
4. Choose a tag version (e.g., `v1.0.0`)
5. Add a release title (e.g., "Initial Streamlit in Snowflake Support Conversations Analysis")
6. Add a description of the features
7. Click "Publish release"

## GitHub Repository Structure

After pushing, your repository structure should look like this:

```
Repository Root/
├── SiS Code/
│   ├── README.md
│   ├── STREAMLIT_DEPLOYMENT.md
│   ├── GITHUB_STRATEGY.md
│   ├── requirements_streamlit.txt
│   ├── support_conversations_app.py
│   └── support_conversations_app_simplified.py
└── ... (other repository files)
```

## Best Practices for Future Updates

1. **Create Branches for New Features**:
   ```bash
   git checkout -b feature/new-visualization
   ```

2. **Pull Latest Changes Before Working**:
   ```bash
   git pull origin main
   ```

3. **Regularly Commit Changes with Clear Messages**:
   ```bash
   git commit -m "Add new device breakdown visualization"
   ```

4. **Create Pull Requests for Code Review** (if collaborating with others)

5. **Tag Major Versions** when releasing significant updates 