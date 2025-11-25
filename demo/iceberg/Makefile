.PHONY: plugin
plugin:
	rm -r plugin || true
	mkdir -p plugin
	../../gradlew -p ../../ :core:distTar :storage:core:distTar :storage:filesystem:distTar :storage:s3:distTar
	tar -xf ../../core/build/distributions/core-*.tgz -C plugin --strip-components=1
	tar -xf ../../storage/core/build/distributions/core-*.tgz -C plugin --strip-components=1
	tar -xf ../../storage/filesystem/build/distributions/filesystem-*.tgz -C plugin --strip-components=1
	tar -xf ../../storage/s3/build/distributions/s3-*.tgz -C plugin --strip-components=1
