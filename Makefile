.PHONY: build help lint

build: package_py ## build artifacts

# God bless the interwebs:
# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
help: ## List Makefile targets
	$(info Makefile documentation)
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'

package_py: ## Make a python package
	# Requires uv >= 0.7.0
	echo "__version__ = \"$$(echo $$(uv version) | sed 's/.*\s//')\"" > command_runner/version.py
	uv build

lint: ## lint the code
	uv run black command_runner
	uv run pylint command_runner
	uv lock
