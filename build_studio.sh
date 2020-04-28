mvn clean install
cp ./target/thoughtspot-component-0.0.1-SNAPSHOT.car .
rm /Applications/TalendStudio-7.2.1/studio/configuration/.m2/repository/com/thoughtspot/load-utility/0.0.1/load-utility-0.0.1.jar 
rm /Applications/TalendStudio-7.2.1/studio/configuration/.m2/repository/com/thoughtspot/thoughtspot-component/0.0.1-SNAPSHOT/thoughtspot-component-0.0.1-SNAPSHOT.jar
java -jar ./thoughtspot-component-0.0.1-SNAPSHOT.car studio-deploy /Applications/TalendStudio-7.2.1/studio
#mvn talend-component:deploy-in-studio -Dtalend.component.studioHome="/Applications/TalendStudio-7.2.1/studio"
