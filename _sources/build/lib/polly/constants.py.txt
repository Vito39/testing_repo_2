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
