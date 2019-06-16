databases = {
    "LBG-DEV-SRV-01": {
        "server"           : "LBG-DEV-SRV-01",
        "default_database" : "BranchAuditCopyOfLive",
        "databases"        : ["BranchAuditCopyOfLive", "BranchHardwareCopyOfLive"]
    },
    "LBG-PRD-SRV-01": {
        "server"           : "LBG-PRD-SRV-01",
        "default_database" : "BranchAudit",
        "databases"        : ["BranchAudit", "BranchHardware"],
    },
    "OMSSQL07": {
        "server"           : "OMSSQL07",
        "default_database" : "049_Flybe",
        "databases"        : ["049_Flybe", "049_Flybe_Campaigns"]
    },
    "FLYBE-PRD-SRV": {
        "server"           : "FLYBE-PRD-SRV",
        "default_database" : "049_Flybe",
        "databases"        : ["049_Flybe", "049_Flybe_Campaigns", "049_Flybe_EmarsysRDS"]
    },
    "SHODAN": {
        "server"           : R".\MattDb",
        "default_database" : "Matt",
        "databases"        : ["Matt"]
    }
}
