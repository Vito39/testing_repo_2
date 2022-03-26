DATA_TYPES = {
    "Mutation": [
        {
            "format": ["maf"],
            "supported_repo": [
                {
                    "name": "cbioportal",
                    "header_mapping": {
                        "gene": "Hugo_Symbol",
                        "chr": "Chromosome",
                        "startPosition": "Start_Position",
                        "endPosition": "End_Position",
                        "referenceAllele": "Reference_Allele",
                        "variantAllele": "Tumor_Seq_Allele2",
                        "mutationType": "Variant_Classification",
                        "variantType": "Variant_Type",
                        "uniqueSampleKey": "Tumor_Sample_Barcode",
                    },
                },
                {"name": "tcga", "header_mapping": {}},
            ],
        }
    ]
}

import os

HOME_PATH = os.getenv("HOME")
DISCOVER_CREDS_FOLDER_PATH = HOME_PATH + "/.discover/credentials"

# endpoints
CONSTANTS_ENDPOINT = "/constants"
REPOSITORIES_ENDPOINT = "/repositories"
REPOSITORY_PACKAGE_ENDPOINT = REPOSITORIES_ENDPOINT + "/{}/packages"

# statuscodes
OK = 200
CREATED = 201