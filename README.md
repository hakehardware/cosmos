# cosmos

## Feature Roadmap

### MVP 1

1. Ability to remotely connect to your node & farmer's metrics endpoint
1. Ability to view basic information about your node & farmer
1. Ability to monitor wallet balance for coinbases
1. Single dashboard to monitor all nodes/farmers/wallets

### MVP 2
1. Discord hooks for transfers into and out of wallet
1. Individual page for each node/farmer/wallet


## Functionality
When the python script is executed in Docker it needs to do the following:

### Initialize the Database + Tables
If the tables and database are already created, then the creation step will be skipped.

When the script first starts, a call to self.database_api.initialize() will occur. This call will create a database called cosmos with the following tables:
1. events - This table holds all of the log entries
2. farmer - This table will hold information on the farmer, typically this has just 1 row
3. farms - This table holds information on each farm
4. rewards - This table will hold information on each reward
5. plots - This table will hold information on each plotting and replotting event


### Add the farmer information to the farmer table
When the script is started, and after the intiailization step, it will verify that the farmer exists in the database. It does this by pulling the farmer_name from the config and querying the farmer table to see if the farmer exists. If it does not, then a new row is inserted with the farmer information. If you change the config, the old farmer will be deleted and the new farmer will be added. Currently, only one farmer is supported as there is no benefit to running more than one farmer per server.

### New logs are added to the event table
Specific events from the logs are tracked in the events table. On start up, the script will attempt to find any new events from the logs and add them to the events table. Depending on how many untracked events exists, this may take 1-2 minutes. 

### Farms added to the farms table
In Subspace, typically each drive you add is a farm. This is typically what you specify in docker as:
```"path=/subspace01,size=1.8T"```

You can sub-divide a drive into more farms if you wish and it will work fine with cosmos. Cosmos gets the farm data in two ways. When the logs are parsed each farm is listed along with its farm_index and farm_id. The promtheus metrics endpoint also has the farm_id listed.

Currently, the metrics endpoint is used to find the farm_id for all of the farms reported. These are then added to the farms table. Any farm_ids that are not reported by the metrics endpoint are removed from the farms table to keep things clean.


### Backfill Logs
At this point the farm may not have a farm_index associated with it. In order to process most logs we need a farm_index as they do not use a farm_id. When we backfill the logs, we will start with the oldest first and will be able to associate the farm_index with the farm_id if it hasn't been done already.

During the backfill we will continue to populate the farms table, along with the rewards and plots table.

### Monitoring Logs
Once all the backfills are complete, the script switches to real time monitoring where logs are ingested and the appropriate tables and rows updated as needed.

### Monitoring Metrics
At the same time as log monitoring, a task is kicked off to monitor metrics and update the appropriate tables and rows. 

### Data Persistence
Because the logs are stored in the database, if you were to stop cosmos and start it again, it would do another backfill to ensure it has the most up-to-date information. If you switched farms resulting in different farm_indexes being associated with a different farm_id then those updates would be made during the log backfill. Furthermore, if farm_ids are no longer used, or new ones are added, this will also happen during the initialization process as all farm_ids not actively being reported by metrics are removed from the database, and new ones added.

### Wiping Data
If for some reason you want to reset your metrics, you will need shut off cosmos and manually wipe the applicable rows via the sqlite. In the future, this functioniality we be accomplished via the API, and could be integrated into the web or console front end.










1. The new logs are added to the event table
1. Add the farms to the farms table
    1. This information is first added by the metrics endpoint
    1. Then the logs are used to assign the farm_index to the farm_id
    1. Additional information will be added by parsing the events from the event table later
1. A loop to pull metrics will be kicked off. This will pull all metrics and add them to the database every X minutes
1. The logs will be monitored for new events, and the applicable databases will be updated.
