# s3-key-finder

## usage
All configuration is defined in the appsettings.json
All found keys are written to a local CSV file

#### basic configuration
- **region**: your s3 region to operate against
- **accessKey**: your s3 accessKey for authentication
- **secretKey**: your s3 secretKey for authentication
- **bucketName**: your s3 bucket name to operate against

#### filtering results
- **minSizeBytes**: min size threshold to filter object results
  - min: 0
  - max: long.MaxValue
  - set to -1 to ignore filter
- **maxSizeBytes**: max size threshold to filter object results
  - min: 0
  - max: long.MaxValue
  - set to -1 to ignore filter
- **keyPattern**: c# regular expression pattern to filter object results

#### operating on results
- **action**: Name of the post-find action
  - Options: "DELETE", "RENAME"
- **dryRun**: When set to true, does not perform any PUT calls to s3
- **batchSize**: The maximum number of s3 calls to make in parallel for a given action
  - Note: this does not guarantee the number of parallel calls
- **settings**: action specific settings (see **actions**)

#### actions
(* = actions setting is required)

- **Delete**: Deletes all found objects, and writes successful ops to CSV
- **Rename**: Copies all found objects, and writes successful ops to CSV
  - settings
    - find*: a c# regular expression to match in the object key
	- replace*: the string replacement for matches in the object key
	- deleteSource: when set to true, the source object is deleted on a successful copy

## publish project
```
dotnet publish -r win-x64 -p:PublishSingleFile=true --self-contained true -c release
```