# GitHub-ETL
An End to end ETL Data pipeline that fetch all the commits from a project on GitHub, read the response object and do DB updates accordingly.

The pipeline takes to arguments date and URL of the required GitHb respo then fetch data starting from the first day of the month in the date until that date.
After pasrsing the returned object array, the pipeline extract the fields and load them into the Postgres tables structured as `initdb.sql`

The pipeline handles the cases that if the database already has records in the same date it should not insert duplicate records.

More enhnacment and edge case would be implemented later to cover error handling and edge cases.