# version 0.0.4
# note: you may need to install the following two libraries
# install.packages("AzureStor")
# install.packages("rjson")
library("AzureStor")
library("rjson")


list_datalake_files <- function(datalake_name, container_name, settings_path=NULL, data_dir=NULL, failure_ok=FALSE) {

  tryCatch(
    expr = {
      # connect to the data lake
      datalake_token <- get_datalake_token(settings_path)
      storage_endpoint_url <- paste("https://" , datalake_name, ".blob.core.windows.net", sep='')
      ad_endp_tok <- storage_endpoint(storage_endpoint_url, token=datalake_token)
      cont <- storage_container(ad_endp_tok, container_name)

      # list the files in the data lake
      if(is.null(data_dir)) {
        return(list_blobs(cont, info='all'))
      } else {
        return(list_blobs(cont, data_dir, info='all'))
      }
    } ,
    error = function(e) {

      print('Unable to list files from ADLS')
      message(e)
      if(failure_ok != TRUE) {
        stop(e)
      }
    }
  )

}

delete_datalake_file <- function(datalake_name, container_name, file_to_delete, settings_path=NULL, failure_ok=FALSE) {

  tryCatch(
    expr = {

      # connect to the data lake
      datalake_token = get_datalake_token(settings_path)
      storage_endpoint_url = paste("https://" , datalake_name, ".blob.core.windows.net", sep='')
      ad_endp_tok <- storage_endpoint(storage_endpoint_url, token=datalake_token)
      cont <- storage_container(ad_endp_tok, container_name)


      # delete file
      delete_storage_file(cont, file_to_delete)

    } ,
    error = function(e) {

      print('Unable to delete file from ADLS')
      message(e)
      if(failure_ok != TRUE) {
        stop(e)
      }
    }

  )

}

download_from_datalake <- function(datalake_name, container_name, datalake_path, download_path, settings_path=NULL, overwrite_download_path=FALSE, check_if_file_older_than_hours=NULL, failure_ok=FALSE) {

  tryCatch(
    expr = {
      # connect to the data lake
      datalake_token <- get_datalake_token(settings_path)
      storage_endpoint_url <- paste("https://" , datalake_name, ".dfs.core.windows.net", sep='')
      ad_endp_tok <- storage_endpoint(storage_endpoint_url, token=datalake_token)
      cont <- storage_container(ad_endp_tok, container_name)


      if(is.null(check_if_file_older_than_hours) == FALSE) {
        current_datetime <- Sys.time()
        attr(current_datetime, "tzone") = "GMT"
        hrs_in_seconds <- check_if_file_older_than_hours * 60 * 60
        datetime_to_check = current_datetime - hrs_in_seconds

        data_lake_file_info = list_datalake_files(datalake_name, container_name, settings_path, data_dir=datalake_path, failure_ok=failure_ok)
        modified_time = data_lake_file_info$`Last-Modified`

        adls_file_is_too_old = datetime_to_check > modified_time

        if(adls_file_is_too_old == TRUE && failure_ok == FALSE) {
          print('Datalake file is too old. Please check')
          stop('Datalake file check failed')

        }
      }


      storage_download(cont, datalake_path, download_path, overwrite=overwrite_download_path)


    } ,
    error = function(e) {

      print('Unable to download file from ADLS')
      print(datalake_path)
      message(e)
      if(failure_ok == TRUE) {
        if(overwrite_download_path == TRUE) {
          file.remove(download_path)
        }
      } else {
        stop(e)
      }

    }

  )

}

download_files_from_datalake <- function(datalake_name, container_name, datalake_paths_download_paths_df, settings_path=NULL, overwrite=FALSE, failure_ok=FALSE) {

  tryCatch(
    expr = {
      # connect to the data lake
      datalake_token <- get_datalake_token(settings_path)
      storage_endpoint_url <- paste("https://" , datalake_name, ".dfs.core.windows.net", sep='')
      ad_endp_tok <- storage_endpoint(storage_endpoint_url, token=datalake_token)
      cont <- storage_container(ad_endp_tok, container_name)

      print('Downloading files from datalake')
      for (row in 1:nrow(datalake_paths_download_paths_df)) {
        datalake_path <- datalake_paths_download_paths_df[row,"datalake_path"]
        download_path <- datalake_paths_download_paths_df[row,"download_path"]
        print('Downloading download_path: %s to download_path: %s',datalake_path, download_path)
        storage_download(cont, datalake_path, download_path, overwrite=overwrite)
      }


    } ,
    error = function(e) {

      print('Unable to download files from ADLS')
      message(e)
      if(failure_ok != TRUE) {
        stop(e)
      }
    }

  )

}

upload_to_datalake <- function(datalake_name, container_name, dest_path, src_path, settings_path=NULL, overwrite=FALSE, metadata_owner=NULL, metadata_short_descr=NULL, metadata_documentation_url=NULL, metadata_data_dict_url=NULL, metadata_tags=NULL, failure_ok=FALSE, upload_version=FALSE){

  tryCatch(
    expr = {

      # connect to the data lake
      datalake_token <- get_datalake_token(settings_path)
      storage_endpoint_url <- paste("https://" , datalake_name, ".dfs.core.windows.net", sep='')
      ad_endp_tok <- storage_endpoint(storage_endpoint_url, token=datalake_token)
      cont <- storage_container(ad_endp_tok, container_name)

      # upload file to ADLS
      if(overwrite == FALSE && blob_exists(cont, dest_path) == TRUE) {
        print('ADLS file already exist and overwrite=FALSE Skipping upload')
      }
      else {
        print('Uploading file to ADLS')
        storage_upload(cont, src=src_path, dest=dest_path)
        print(dest_path)
        # update metadata if provided
        if(is.null(metadata_owner) == FALSE) {
          set_storage_metadata(cont, dest_path, owner=metadata_owner)
        }
        if(is.null(metadata_documentation_url) == FALSE) {
          set_storage_metadata(cont, dest_path, documentation_url=metadata_documentation_url)
        }
        if(is.null(metadata_short_descr) == FALSE) {
          set_storage_metadata(cont, dest_path, description=metadata_short_descr)
        }
        if(is.null(metadata_data_dict_url) == FALSE) {
          set_storage_metadata(cont, dest_path, data_dict_url=metadata_data_dict_url)
        }
        if(is.null(metadata_tags) == FALSE) {
          set_storage_metadata(cont, dest_path, tags=metadata_tags)
        }

        if(upload_version==TRUE) {
          print('Uploading versioned file to ADLS')
          currentTime <- Sys.time()
          attr(currentTime, "tzone") = "GMT"
          number_of_splits = length(strsplit(dest_path, split = "/")[[1]])
          file_name = strsplit(dest_path, split = "/")[[1]][number_of_splits]
          version_path = format(currentTime, paste0("versions/YEAR=%Y/MONTH=%m/DAY=%d/%Y-%m-%d-%Y:%H:%M:%S:%z_", file_name))
          versioned_dest_path = gsub(file_name, version_path, dest_path)
          storage_upload(cont, src=src_path, dest=versioned_dest_path)
          print(versioned_dest_path)
          if(is.null(metadata_owner) == FALSE) {
            set_storage_metadata(cont, versioned_dest_path, owner=metadata_owner)
          }
          if(is.null(metadata_tags) == FALSE) {
            set_storage_metadata(cont, versioned_dest_path, tags=metadata_tags)
          }

        }
      }

    } ,
    error = function(e) {

      print('Unable to upload file to ADLS')
      message(e)
      if(failure_ok != TRUE) {
        stop(e)
      }
    }

  )

}

get_datalake_token <- function(settings_path=NULL) {
  tenant_id <- NULL
  client_id <- NULL
  client_secret <- NULL
  # try to load settings from file
  if (!is.null(settings_path)) {
    settings_file <- fromJSON(file = settings_path)
    tenant_id <- settings_file[['AZURE_TENANT_ID']]
    client_id <- settings_file[['AZURE_CLIENT_ID']]
    client_secret <- settings_file[['AZURE_CLIENT_SECRET']]
  }

  # if the file doesn't have a secret check the environment
  if(Sys.getenv('AZURE_TENANT_ID') != "") {
        tenant_id <-Sys.getenv('AZURE_TENANT_ID')
  }
  if(Sys.getenv('AZURE_CLIENT_ID') != "") {
        client_id <-Sys.getenv('AZURE_CLIENT_ID')
  }
  if(Sys.getenv('AZURE_CLIENT_SECRET') != "") {
        client_secret <-Sys.getenv('AZURE_CLIENT_SECRET')
  }

  if(is.null(tenant_id) && is.null(client_id) && is.null(client_secret)) {
    tryCatch(
    expr = {
        az_login <- AzureRMR::get_azure_login()
    },
    error = function(e){
        print(paste('No azure login found, please login', e))
        AzureRMR::create_azure_login()
    }
  )
    az_login <- AzureRMR::get_azure_login()
    token <- AzureRMR::get_azure_token("https://storage.azure.com", tenant='common', app=az_login$token$client$client_id)

  } else {

  token <- AzureRMR::get_azure_token("https://storage.azure.com",
                                     tenant=tenant_id,
                                     app=client_id,
                                     password=client_secret)
  }
  return(token)
}
