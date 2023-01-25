from pulsar import Function
import re


class PreprocessFunction(Function):
    def __init__(self):
        self.output_topic = "persistent://twitter-test-1/default/preprocess-topic"

    def process(self, input, context):
        try:
            tweet = re.sub('@[^\s]+','', str(input))
            
            emoticon_pattern = re.compile("["
            u"\U0001F600-\U0001F64F"  
            u"\U0001F300-\U0001F5FF"  
            u"\U0001F680-\U0001F6FF"  
            u"\U0001F1E0-\U0001F1FF"  
                            "]+", flags=re.UNICODE)
        
            tweet = (emoticon_pattern.sub(r'', str(tweet)))
            tweet = re.sub(r'http\S+', '', str(tweet))
            
            context.publish(self.output_topic, "Pre-processed tweet: {}".format(str(tweet)))


        except:
            warning = "Negaaahtive Ghostrider. {}".format(input)
            context.get_logger().warn(warning)
