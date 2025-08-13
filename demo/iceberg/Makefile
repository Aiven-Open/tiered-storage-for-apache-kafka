.PHONY: plugin
plugin:
	cd ../../ && ./gradlew :core:distTar
	cd ../../ && ./gradlew :storage:core:distTar
	cd ../../ && ./gradlew :storage:filesystem:distTar
	rm -r plugin || true
	mkdir -p plugin
	tar -xf ../../core/build/distributions/core-0.0.1-SNAPSHOT.tgz -C plugin --strip-components=1
	tar -xf ../../storage/core/build/distributions/core-0.0.1-SNAPSHOT.tgz -C plugin --strip-components=1
	tar -xf ../../storage/filesystem/build/distributions/filesystem-0.0.1-SNAPSHOT.tgz -C plugin --strip-components=1
