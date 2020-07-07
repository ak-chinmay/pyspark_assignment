#Assignment Solution
By Chinmay Arankalle (ak.chinmay@gmail.com)
###Finding similar customers

### PySpark Solution

Thank you for giving me the opportunity to take the test, below is the description of the solution.
In this solution I have created the PySpark based workflow which reads json file,<br/> performs transformations and 
displays it on STDOUT.

Below are the technologies which I have used and will be required to run the workflow:

##### Requirements

- Python 3.7.x
- Spark 2.4 (local single node or multi-node cluster)
##### Approach

This program following approach as below:
1. Read the source JSON file as dataframe
2. Filters the dataframe as per customer id
3. Retrieves the values of `attributes` column in form of set data structure<br/>
 so that we will be able to search through this set with `O(1)` complexity for rest of the records
4. Retrieves the keys/field names of `attributes` column
5. Used these field names to concat the rest of the values of attributes column
6. This gave a string inside a column whose value we can check against the set
7. Hence now we know how many of the fields from 'set' matches for a specific records,
that is our `num_similar_attributes`
8. This new field is used to perform order by in descending mode
9. Results are are shown on console, which can changed
10. This program dynamically checks similarity for all the fields of attributes column,
even though in future you more or less number of fields it should work fine
###### Enhancements

1. We could partition the processed dataframe and save in parquet format,
 so that we only get the required column in memory while checking against the `set`
2. This program can be extended to save the result as parquet or csv, methods are already implemented

##### CICD Approach

1. In order to implement CICD for this solution we need to understand and decide the technology to use
there are plenty of CICD technologies out there in market like Jenkins and Gitlab
2. I have used Gitlab so I could explain this use case using it.
3. We need to have .gitlab-ci.yml file which will orchestrate the CICD pipeline, We can target branches like dev and master 
when feature or hotfix branches are merged into these branches the CICD pipeline could kick in (or even on the feature branches push we can start the pipeline as per the requirement).
4. Gitlab we can then spawn Docker container to run all the tests, usually I use bash scripts to execute custom operations like 
running tests, linting with SonarQube and use `scp` the newly generated artifact to artifactory like JFrog
5. With Python we can have distributable packages i.e. .egg files which consists of executable code
6. If we want to start EMR after the successful completion of CI pipeline then there are two ways to do this.
    a. We can store the artifacts in AWS S3 and use SNS -> Lambda connection to start the EMR once the .egg file is added in S3 bucket.
    This will give us freedom to be able to configure Lambda and SNS as per the requirement, EMR configuration could also be done as per feature or hotfix.
    b. Alternative approach is by using Docker, we can use put artifactory in docker images and store the image in AWS Container registry,
     I think in the new version of EMR it is possible to use docker image to while spawning the EMR.
7. If the pipeline fails/succeeded then all the stakeholders will get an email notification
8. CICD pipelines can be designed by considering the requirements of branches such as release, feature, hotfix. The pipeline should be configurable and also EMR configurations should be highly dependant on the changes or data to be used.


##### Operations

1. There is a ZIP file which contains the source code, extract it
2. You need to install the dependencies, enter following command in the root directory:

    `python3 setup.py install`

   This will install the dependencies.
   Note: You might need to install unittest separately

3. config.json should be put either in root directory or near .egg package as per your choice of execution
4. Also, you need to Provide running Spark instance **master url** in config.json, path to the source file and destination path
5. Once dependencies are installed, you can run program.<br/>
There are three ways to run the program
 
    **a.** By using .egg file, Once done, Please run following to read the help doc of the project which provides an information about the project arguments.<br/>which is the executable and distributable package.<br/>
    To create an egg file in our project enter following command:
   
   `python3 setup.py bdist_egg`

    This command will create the .egg package inside dist folder. You will need the properties file 
    **config.json** and the bash file **run.sh**. 
    <br/>You can copy these files inside dist folder.
5. You can run the program in 3 modes

    a. `python __main__.py -c <customer_id>`
    b. `python assignment-0.1-py3.7.egg -c <customer_id>`
    c. `bash run -c <customer_id>`

6. The program has argument parsing mechanism so you can pass '-h' argument to see the help section.
`python __main__.py -h`<br/>
Also, input file can be passed with -f flag
`python __main__.py -f <input_file_path>` if not passed then program will access it from default location which is `tests/resources`

7. I have included tests in the prorgram, all the tests can be run with following command

`python __main__.py --test`

All these arguments are applicable for all the running modes

8. Once you run the program you will it will submit the job to Spark cluster you chose
and show the results on console.


##### Modules and code understanding

The project contains following packages and modules:

1. tasks : This package contains the business logic of the project
2. analytics : This package contains the query execution logic<br/>
    2.1 similarity : This module has the business logic to find similar customers
    2.2 filter : This module executes filter logic<br/>
    2.3 sql_exec : This module executes the given Spark-SQL query
    2.3 udf : This module creates required UDF
3. io : This package contains the io read write logic<br/>
    3.1 spark_connection : This module connects to Spark cluster<br/>
    3.2 table : This module creates temp view<br/>
    3.3 read : This module reads the input file<br/>
    3.4 write : This module writes the dataframe to json, parquet or displays it on STDOUT
4. util : This package contains the Logging and Properties file logic
    4.1 process_log : This module generates the logs
    4.2 properties_reader : This reads the properties file
5. director.py : This is the main module which is responsible for orchestration of all the activities

##### Reference

The clean code guidelines are followed as per [PEP8](https://www.python.org/dev/peps/pep-0008/) 
