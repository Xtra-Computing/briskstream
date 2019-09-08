mvn install:install-file \
  -DgroupId=ch.usi.overseer \
  -DartifactId=overseer-java-api \
  -Dpackaging=jar \
  -Dversion=1.0-SNAPSHOT \
  -Dfile=src/java/overseer.jar \
  -DgeneratePom=true