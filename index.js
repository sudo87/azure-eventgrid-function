// FunctionName: eventgridCreateBinaryStreamMetadata
// Purpose: When a file is uploaded to an RDP media asset bucket for a tenant, the bucket should trigger this function. 
// It Will create binary stream object that contains the metadata of the file. 
// It will invoke a REST API call on RDP API Server.
// Version: 18.10
// Last update: Oct-15-2018, eventgrid integrated function

'use strict';

var isDebugEnabled = false;
var http = require('http');
const storage = require('azure-storage');

const TASK_ID_METADATA_PROPERTY = 'x-rdp-taskid';
const RDP_ESCAPING_PREFIX = 'x_rdp_';
const RDP_PREFIX = 'x-rdp-';

module.exports = function (context, eventGridEvent) {
    if (isDebugEnabled) {
        context.log("Eventgrid event: ", eventGridEvent);
    }


    // obtain subject value from event
    // e.g. '/blobServices/default/containers/rdp-media-assets-engg-az-qa5-rwtest/blobs/renditions/8thoct5k2ndset4048_twoHundred.tiff'
    var subjectVal = eventGridEvent.subject.split("/");
    // avoid any subdirectory blob processing
    if (subjectVal.length == 7) {

        var containerName = subjectVal[4]
        var fileBlobName = subjectVal.pop();

        // get env Config
        var envConfig = getEnvironmentConfiguration(context);

        if (isDebugEnabled) {
            context.log.info("Container name: ", containerName);
            context.log.info("blobName : ", fileBlobName);
            context.log.includes("Env config: ", envConfig);
        }

        if (envConfig && envConfig.StorageConStr) {
            var StorageCreds = envConfig.StorageConStr;
            const blobService = storage.createBlobService(StorageCreds);

            // get blob metadata
            blobService.getBlobMetadata(containerName, fileBlobName,
                function (error, blobMetadataResult) {
                    if (error) {
                        context.log("Error fetching metadata");
                        context.log("subjectVal: ", subjectVal);
                        context.done();
                    } else {
                        if (blobMetadataResult) {
                            if (isDebugEnabled) {
                                context.log('BlobMetaResult: ', JSON.stringify(blobMetadataResult, null, 2));
                            }

                            var blobMetadata = getBlobMetadata(blobMetadataResult);
                            if (blobMetadata && blobMetadata['x-rdp-tenantid']) {
                                var headers = createRequestHeaders(envConfig, blobMetadata);
                                var binaryStreamObject = buildBinaryStreamObject(context, eventGridEvent, blobMetadataResult, headers);
                                postBinaryStreamObject(context, envConfig, headers, binaryStreamObject);
                            } else {
                                const errorMsg = "TenantId is not present in asset metadata";
                                context.log.error(errorMsg);
                            }
                        }
                    }
                });
        }
    }
    context.done();
}

var getBlobMetadata = function (blobMetadataResult) {
    var blobMetadata = {};
    var incomingBlobMetaData = blobMetadataResult.metadata;
    for (var metadataKey in incomingBlobMetaData) {
        var metadataValue = incomingBlobMetaData[metadataKey];
        blobMetadata[(metadataKey.replace(RDP_ESCAPING_PREFIX, RDP_PREFIX)).toLowerCase()] = metadataValue;
    }
    return blobMetadata;
};

var createRequestHeaders = function (envConfig, blobMetadata) {
    var tenantId = blobMetadata['x-rdp-tenantid'];
    var clientId = getPropertyValueFromMetadata(blobMetadata, 'x-rdp-clientid', envConfig.defaultClientId);

    var headers = {
        'Content-Type': 'application/json',
        'x-rdp-version': '8.1',
        'x-rdp-clientId': clientId,
        'x-rdp-tenantId': tenantId,
        'x-rdp-userId': blobMetadata['x-rdp-userid'] ? blobMetadata['x-rdp-userid'] : envConfig.defaultUserId,
        'x-rdp-userRoles': blobMetadata['x-rdp-userroles'] ? blobMetadata['x-rdp-userroles'] : envConfig.defaultUserRoles
    };

    if (blobMetadata['x-rdp-ownershipdata']) {
        headers['x-rdp-ownershipData'] = blobMetadata['x-rdp-ownershipdata'];
    }

    return headers;
};


// Extract the metadata properties from blob and if not exists from container
var getPropertyValueFromMetadata = function (blobMetadata, propertyName, defaultValue) {
    var propertyValue = defaultValue;
    if (blobMetadata && blobMetadata[propertyName]) {
        propertyValue = blobMetadata[propertyName];
    }
    return propertyValue;
};


var buildBinaryStreamObject = function (context, eventGridEvent, blobMetadataResult, headers) {
    // The object key for the blob
    var blobObjectKey = blobMetadataResult.name;
    var blobMetadata = blobMetadataResult.metadata;

    var invocationId = context.bindingData.invocationId;

    // We will attempt to get the object id used for binary stream object from the metadata
    // If it does not exists, we will use the invocation id
    var assignedObjectId = blobMetadata.binarystreamobjectid ? blobMetadata.binarystreamobjectid : invocationId;

    // Look for the original file name in the metadata
    // if not found we will use the blob key ( removing any parent folders in case it was delimited by path)
    if (blobMetadata.originalfilename) {
        var assignedOriginalFileName = blobMetadata.originalfilename;
        // Remove it so we won't duplicate it in the binary stream object
        delete blobMetadata.originalfilename;
    } else {
        var assignedOriginalFileName = blobObjectKey.split('/').pop();
    }

    // Assign taskId based on the object tag if exists otherwise use the invocation id
    var taskId = invocationId;
    if (blobMetadata && blobMetadata.TASK_ID_METADATA_PROPERTY) {
        taskId = blobMetadata.TASK_ID_METADATA_PROPERTY;
    }

    var binaryStreamObject = {
        'clientAttributes': {
            'taskId': {
                'values': [{
                    'locale': 'en-US',
                    'source': 'internal',
                    'value': taskId
                }]
            }
        },
        'binaryStreamObject': {
            'id': assignedObjectId,
            'type': 'binarystreamobject',
            'properties': {
                'objectKey': blobObjectKey,
                'originalFileName': assignedOriginalFileName,
                'fullObjectPath': blobObjectKey,
                'contentSize': eventGridEvent.data.contentLength,
                'user': headers['x-rdp-userId'],
                'role': headers['x-rdp-userRoles']
            }
        }
    };

    if (headers['x-rdp-ownershipData']) {
        binaryStreamObject.binaryStreamObject.properties['ownershipData'] = headers['x-rdp-ownershipData'];
    }

    // Apply the metadata of the object as properties, ignore those that of the headers (starts with x-rdp)
    for (var propertyName in blobMetadata) {
        if (!propertyName.includes('x-rdp') && !binaryStreamObject.binaryStreamObject.properties.hasOwnProperty(propertyName)) {
            if (isDebugEnabled) {
                context.log.verbose("Adding metadata property: " + propertyName + ", Value: " + blobMetadata[propertyName]);
            }
            binaryStreamObject.binaryStreamObject.properties[propertyName] = blobMetadata[propertyName];
        }
    }
    return binaryStreamObject;
}


var postBinaryStreamObject = function (context, envConfig, headers, binaryStreamObject) {

    var options = getHttpRequestOptions(envConfig, headers);

    if (isDebugEnabled) {
        context.log.verbose('BinaryStreamObject: ', JSON.stringify(binaryStreamObject, null, 2));
        context.log.verbose('Http request options: ', JSON.stringify(options, null, 2));
    }

    var responseBody = '';
    // Prepare the post request
    var req = http.request(options, function (res) {
        if (isDebugEnabled) {
            context.log.verbose('Status: ' + res.statusCode);
            context.log.verbose('Headers: ' + JSON.stringify(res.headers));
        }

        // Handle on recieving data, which we should ingore
        res.on('data', function (chunk) {
            responseBody += chunk;
        });

        // On Error
        res.on('error', function (e) {
            context.log.error("Error while calling REST API: " + e);
            context.done("Error while calling REST API: " + e);
        });

        var taskId = binaryStreamObject.clientAttributes.taskId.values[0].value;

        // On end
        res.on('end', function () {
            var isSuccessfull = false;
            if (res.statusCode == 200 && responseBody) {
                if (isDebugEnabled) {
                    const responseMessage = `Using taskId ${taskId}, RDP API Response: ${responseBody}`;
                    context.log(responseMessage);
                }
                var responseJson = JSON.parse(responseBody);
                if (responseJson && responseJson.response && responseJson.response.status && responseJson.response.status.toLowerCase() == 'success') {
                    isSuccessfull = true;
                }
            }

            if (isSuccessfull) {
                context.done();
            } else {
                const errorMsg = "Fail to make REST api call to post the binary stream object.";
                context.log.error(errorMsg);
                context.done(errorMsg);
            }
        });
    });

    req.write(JSON.stringify(binaryStreamObject));
    req.end();
};

var getHttpRequestOptions = function (envConfig, headers) {
    var options = {
        'protocol': 'http:',
        'host': envConfig.rdpHost,
        'port': envConfig.rdpPort,
        'path': '/' + headers['x-rdp-tenantId'] + '/api/binarystreamobjectservice/create',
        'method': 'POST',
        headers
    };
    return options;
};

var getEnvironmentConfiguration = function (context) {
    if (!process.env.StorageConStr) {
        const errorMsg = "Unable to locate environment variable StorageConStr";
        context.log.error(errorMsg);
        context.done(errorMsg);
        return null;
    }

    if (!process.env.ENV_RDP_HOST) {
        const errorMsg = "Unable to locate environment variable ENV_RDP_HOST";
        context.log.error(errorMsg);
        context.done(errorMsg);
        return null;
    }

    if (!process.env.ENV_RDP_PORT) {
        const errorMsg = "Unable to locate environment variable ENV_RDP_PORT";
        context.log.error(errorMsg);
        context.done(errorMsg);
        return null;
    }

    if (!process.env.ENV_CLIENT_ID) {
        const errorMsg = "Unable to locate environment variable ENV_CLIENT_ID";
        context.log.error(errorMsg);
        context.done(errorMsg);
        return null;
    }

    if (!process.env.ENV_DEFAULT_USER_ID) {
        const errorMsg = "Unable to locate environment variable ENV_DEFAULT_USER_ID";
        context.log.error(errorMsg);
        context.done(errorMsg);
        return null;
    }

    if (!process.env.ENV_DEFAULT_USER_ROLES) {
        const errorMsg = "Unable to locate environment variable ENV_DEFAULT_USER_ROLES";
        context.log.error(errorMsg);
        context.done(errorMsg);
        return null;
    }

    // Get all the env variables
    var envConfig = {
        'rdpHost': process.env.ENV_RDP_HOST,
        'rdpPort': process.env.ENV_RDP_PORT,
        'defaultClientId': process.env.ENV_CLIENT_ID,
        'defaultUserId': process.env.ENV_DEFAULT_USER_ID,
        'defaultUserRoles': process.env.ENV_DEFAULT_USER_ROLES
    };
    return envConfig;
};