# couchbaseassignment
Assignment for data load tool

        Format to run:  java DataLoadTool <<no_of_docs>> <<doc_size>>
            <no_of_docs> -> Number of JSON documents needed to populate in one batch
            <doc_size>> -> Document size in Bytes (this value can be any number but more than 15)

        Eg:  If you wan to poulate 10 docs each of 20 Bytes
             
             java DataLoadTool 10 20

