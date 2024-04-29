# Maintainer Guidance

## Publish package to pypi
When we are ready to release a new version `vx.y.z`, one of the maintainers should:
1. Edit the `pyproject.toml` file, update the `[tool.poetry].version = vx.y.z`
2. Create a new tag `git tag vx.y.z`, if the tag doesn't match with the project version, step 4 validation will fail  
3. push the tag to the repo `git push origin vx.y.z`
4. There will be a GitHub Action triggered automatically, which will build and publish to pypi