class CedaEOQueries():

    def basic(self):
        return {
            "_source": {
                "include": [
                    "data_format.format",
                    "file.filename",
                    "file.path",
                    "file.data_file",
                    "file.quicklook_file",
                    "file.location",
                    "file.directory",
                    "misc",
                    "spatial",
                    "temporal"
                ]
            },
            "sort": [
                {
                    "temporal.start_time": {"order": "desc"}
                },
                "_score"
            ],
            "query": {
                "bool": {
                    "must": {
                        "match_all": {}
                    },
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "exists": {
                                        "field": "spatial.geometries.display.type"
                                    }

                                }
                            ],
                            "should": [],
                            "must_not": []
                        }
                    }
                }
            },
            "aggs": {
                "data_count": {
                    "terms": {
                        "field": "misc.platform.Satellite.raw"
                    }
                },
                "all": {
                    "global": {},
                    "aggs": {
                        "satellites": {
                            "terms": {
                                "field": "misc.platform.Satellite.raw",
                                "size": 30
                            }
                        }
                    }
                }
            },
            "size": 200
        }

    def geo_shape(self):
        geo_shape_json = {
            "geo_shape": {
                "spatial.geometries.search": {
                    "shape": {
                        "type": "envelope",
                        "coordinates": [[-4.75, 53.12], [25.13, 41.11]]
                    }
                }
            }
        }

        query_base = self.basic()
        query_base['query']['bool']['filter']['bool']['should'].append(geo_shape_json)

        return query_base

    def aggs(self):
        return {
            "aggs": {
                "data_count": {
                    "terms": {
                        "field": "misc.platform.Satellite.raw"
                    }
                },
                "all": {
                    "global": {},
                    "aggs": {
                        "satellites": {
                            "terms": {
                                "field": "misc.platform.Satellite.raw",
                                "size": 30
                            }
                        }
                    }
                }
            },
            "size": 0
        }

    def haveMission(self):
        return {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "exists": {
                                "field": "misc.platform.Mission.keyword"
                            }
                        }
                    ]
                }
            }
        }


class IndexMappings():
    """
    Mappings for key indicies
    """
    @staticmethod
    def cedadiMapping():
        return {
            "settings": {
                "analysis": {
                    "filter": {
                        "min_length_5_filter": {
                            "type": "length",
                            "min": 5,
                            "max": 256
                        },
                        "path_filter": {
                            "type": "word_delimiter",
                            "generate_word_parts": "true",
                            "generate_number_parts": "false",
                            "split_on_numerics": "false"
                        }
                    },
                    "analyzer": {
                        "variable_name_analyzer": {
                            "type": "custom",
                            "tokenizer": "keyword",
                            "filter": [
                                "min_length_5_filter"
                            ]
                        },
                        "path_hierarchy_analyzer": {
                            "type": "custom",
                            "tokenizer": "path_hierarchy"
                        },
                        "path_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "path_filter"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "geo_metadata": {
                    "properties": {
                        "data_format": {
                            "properties": {
                                "format": {
                                    "type": "text"
                                }
                            }
                        },
                        "index_entry_creation": {
                            "properties": {
                                "indexer": {
                                    "type": "text"
                                }
                            }
                        },
                        "file": {
                            "properties": {
                                "filename": {
                                    "type": "text",
                                    "analyzer": "simple",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword"
                                        }
                                    }
                                },
                                "path": {
                                    "type": "text",
                                    "analyzer": "path_analyzer",
                                    "fields": {
                                        "hierarchy": {
                                            "type": "text",
                                            "analyzer": "path_hierarchy_analyzer"
                                        },
                                        "raw": {
                                            "type": "keyword"
                                        }
                                    }
                                },
                                "size": {
                                    "type": "long"
                                }
                            }
                        },
                        "misc": {
                            "properties": {
                                "platform": {
                                    "properties": {
                                        "Satellite": {
                                            "type": "text",
                                            "fields": {
                                                "raw": {
                                                    "type": "keyword"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "parameters": {
                            "include_in_parent": "true",
                            "type": "nested",
                            "properties": {
                                "name": {
                                    "type": "text",
                                    "analyzer": "variable_name_analyzer"
                                },
                                "value": {
                                    "type": "text",
                                    "analyzer": "variable_name_analyzer",
                                    "fields": {
                                        "autocomplete": {
                                            "type": "completion",
                                            "analyzer": "variable_name_analyzer",
                                            "search_analyzer": "simple"
                                        },
                                        "raw": {
                                            "type": "keyword"
                                        }
                                    }
                                }
                            }
                        },
                        "spatial": {
                            "properties": {
                                "geometries": {
                                    "properties": {
                                        "search": {
                                            "type": "geo_shape",
                                            "tree": "quadtree",
                                            "precision": "8km"
                                        },
                                        "full_search": {
                                            "type": "geo_shape",
                                            "tree": "quadtree",
                                            "precision": "8km"
                                        },
                                        "display": {
                                            "properties": {
                                                "coordinates": {
                                                    "type": "double",
                                                    "index": "false"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "temporal": {
                            "properties": {
                                "end_time": {
                                    "type": "date",
                                    "format": "dateOptionalTime"
                                },
                                "start_time": {
                                    "type": "date",
                                    "format": "dateOptionalTime"
                                }
                            }
                        }
                    }
                }
            }
        }

    @staticmethod
    def fbsMapping():
        return {
            "mappings": {
                "file": {
                    "properties": {
                        "info": {
                            "properties": {
                                "directory": {
                                    "type": "keyword",
                                    "fields": {
                                        "analyzed": {
                                            "type": "text"
                                        }
                                    }
                                },
                                "format": {
                                    "type": "text",
                                    "fields": {
                                        "keyword": {
                                            "type": "keyword"
                                        }
                                    }
                                },
                                "md5": {
                                    "type": "keyword"
                                },
                                "name": {
                                    "type": "keyword",
                                    "fields": {
                                        "analyzed": {
                                            "type": "text"
                                        }
                                    }
                                },
                                "name_auto": {
                                    "type": "completion",
                                    "search_analyzer": "simple",
                                    "analyzer": "simple"
                                },
                                "phenomena": {
                                    "properties": {
                                        "names": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
                                        },
                                        "units": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
                                        },
                                        "standard_name": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
                                        },
                                        "var_id": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
                                        },
                                        "agg_string": {
                                            "type": "keyword"
                                        }
                                    }
                                },
                                "location": {
                                    "type": "keyword"
                                },
                                "size": {
                                    "type": "long"
                                },
                                "type": {
                                    "type": "text",
                                    "fields": {
                                        "keyword": {
                                            "type": "keyword",
                                            "ignore_above": 256
                                        }
                                    }
                                },
                                "temporal": {
                                    "properties": {
                                        "end_time": {
                                            "type": "date",
                                            "format": "dateOptionalTime"
                                        },
                                        "start_time": {
                                            "type": "date",
                                            "format": "dateOptionalTime"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
