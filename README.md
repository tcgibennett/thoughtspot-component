# Talend ThoughtSpot Component #
### Author: Thomas Bennett <tbennett@talend.com> ###

As of this writing this component will work with the following Talend platforms
* Talend Studio 7.2.1+
* Talend Cloud

Talend Pipeline Designer is currently in testing for this component and will be validated soon.

## How to setup in your environment ##
On the same machine as your Talend Studio
### PreRequisites ###
1. Install [Git](https://git-scm.com/downloads)
2. Install [Maven](https://maven.apache.org/download.cgi)

### Build and Deploy ###
1. git clone https://github.com/Talend/thoughtspot-component.git
2. git clone https://github.com/Talend/load-utility.git
3. cd into load-utility directory
4. mvn clean install
5. cd into thoughtspot-component directory
6. mvn clean install
7. mvn talend-component:deploy-in-studio -Dtalend.component.studioHome="<Path to Talend Studio>"
EX: /Applications/TalendStudio-7.2.1/studio
8. Edit <Path to Talend Studio>/configuration/config.ini and add the following at the end of the file **talend.component.server.icon.paths=icons/%s_icon32.png,icons/png/%s_icon32.png**
9. Start or Restart Talend Studio

## Documentation ##
### MemSQLTableNameInput

Executes a DB query with a strictly defined order which must correspond to the schema definition.


### ThoughtSpotOutput ###

Reads data incoming from the preceding component in the Job and executes the action defined on a given ThoughtSpot table and/or on the data contained in the table.

ThoughtSpotOutput connects to a given ThoughtSpot instance and bulk loads in that database.


#### ThoughtSpotOutput Standard properties

These properties are used to configure MemSQLOutput running in the Standard Job framework.

The Standard ThoughtSpotOutput component belongs to the Business Intelligence family.


## Basic settings

| **Property type** | Either  **Built-in**  or  **Repository**  . |
| --- | --- |
|   | **Built-in** : No property data stored centrally. |
|   | **Repository** : Select the repository file in which the properties are stored. The fields that follow are completed automatically using the data retrieved. |
| **Use an existing connection** | Select this check box and in the  **Component List**  click the relevant connection component to reuse the connection details you already defined. **Note:**  When a Job contains the parent Job and the child Job, if you need to share an existing connection between the two levels, for example, to share the connection created by the parent Job with the child Job, you have to:|
|  | **1.** In the parent level, register the database connection to be shared in the  **Basic settings**  view of the connection component which creates that very database connection.|
|  | **2.** In the child level, use a dedicated connection component to read that registered database connection. For an example about how to share a database connection across Job levels, see _Talend Studio User Guide_.|
| **host** | Enter the IP or DNS of server |
| **port** | Default is 22. |
| **Username**  and  **Password** | Enter the user authentication data for connecting to the database to be used. |
| **Table** | Enter the name of the table to be written. Note that only one table can be written at a time **If table is not present, then under Custom you can enter the name of the table. The &#39;Create If Not Exists&#39; property must also be selected as this will then create the table in the ThoughtSpot instance. Must have schema.tablename ** |
| **Create table if not exists** | The table is created if it does not exist. |
| **truncate** | If table already exists it will truncate all data before starting load |


## Advanced settings

| **Use batch size** | Select this check box to activate the batch mode for data processing. In the  **Batch Size**  field that appears when this check box is selected, you can type in the number you need to define the batch size to be processed. |
| --- | --- |