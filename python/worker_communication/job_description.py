import json

class JobDescription:
    '''
        JobDescription acts as a middleman, standardizing communication between services.
        You can free to set any attribute to an JobDescription instance, but keep your attributes are simple,
        that can be converted to Json format
    '''
    def __init__(self, **kwargs):
        self.attribute_names = set()
        for key in kwargs:
            setattr(self, key, kwargs[key])
            self.attribute_names.add(key)

    def addAttribute(self, name, value):
        setattr(self, name, value)
        self.attribute_names.add(name)
    
    def add_attribute(self, name, value):
        self.addAttribute(name, value)

    def toJson(self):
        data_pack = dict()
        for attr_name in self.attribute_names:
            data_pack[attr_name] = getattr(self, attr_name)
        return json.dumps(data_pack)
    
    def to_json(self):
        return self.toJson()
    
    @classmethod
    def fromJson(cls, json_string):
        data_pack = json.loads(json_string)
        return JobDescription(**data_pack)
    
    @classmethod
    def from_json(cls, json_string):
        return cls.fromJson(json_string)

    def __getitem__(self, key):
        if key not in self.attribute_names:
            raise KeyError("Key {} not found".format(key))
        return getattr(self, key)