-- We take log files situated in a separate VM
-- TO connect this to ADF, we need a self hosting IR
-- After the Setup, create a container in ADLS called 'nginx'
-- As the first step we load log file from VM into this Container using Pipeline Copy Activity (nginxVmToContainerCopy)
        -- To access the file inside VM, we need to give this command in Powershell of VM:  
                -- C:\Program Files\Microsoft Integration Runtime\5.0\Shared> .\dmgcmd.exe -DisableLocalFolderPathValidation

-- now create the Data flow (dataflow_nginx_SelfHostedIR)
-- Create the table below in Synapse
CREATE TABLE Serverlogs
(
    Remote_addr VARCHAR(200),
    time_local VARCHAR(200),
    Request VARCHAR(200),
    Status int,
    Bytes int
)
select * from Serverlogs