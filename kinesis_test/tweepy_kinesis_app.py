import json
import base64
import time
import traceback
import logging

from amazon_kclpy import kcl

class TweetProcessor(kcl.RecordProcessorBase):
    '''
    Processes the tweets from the kinesis stream and prints
    the tweet text into a log file
    '''

    def __init__(self):

        #setup logger
        self._logger = logging.getLogger('tweepy-kinesis')
        self._logger_file_handler = logging.FileHandler('/home/ubuntu/logs/kinesis_test.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        self._logger_file_handler.setFormatter(formatter)
        self._logger.addHandler(self._logger_file_handler)
        self._logger.setLevel(logging.DEBUG)

        #kinesis configs
        self.SLEEP_SECONDS = 5
        self.CHECKPOINT_RETRIES = 5
        self.CHECKPOINT_FREQ_SECONDS = 60


    def initialize(self, shard_id):

        self.largest_seq = None
        self.last_checkpoint_time = time.time()


    def checkpoint(self, checkpointer, sequence_number=None):
        '''
        Checkpoints with retries on retryable exceptions.

        :type checkpointer: amazon_kclpy.kcl.Checkpointer
        :param checkpointer: A checkpointer provided to either process_records or shutdown

        :type sequence_number: str
        :param sequence_number: A sequence number to checkpoint at.
        '''
        for n in range(0, self.CHECKPOINT_RETRIES):
            try:
                checkpointer.checkpoint(sequence_number)
                return
            except kcl.CheckpointError as e:
                if 'ShutdownException' == e.value:
                    '''
                    A ShutdownException indicates that this record processor should be shutdown. This is due to
                    some failover event, e.g. another MultiLangDaemon has taken the lease for this shard.
                    '''
                    print 'Encountered shutdown execption, skipping checkpoint'
                    return
                elif 'ThrottlingException' == e.value:
                    '''
                    A ThrottlingException indicates that one of our dependencies is is over burdened, e.g. too many
                    dynamo writes. We will sleep temporarily to let it recover.
                    '''
                    if self.CHECKPOINT_RETRIES - 1 == n:
                        sys.stderr.write('Failed to checkpoint after {n} attempts, giving up.\n'.format(n=n))
                        return
                    else:
                        print 'Was throttled while checkpointing, will attempt again in {s} seconds'.format(s=self.SLEEP_SECONDS)
                elif 'InvalidStateException' == e.value:
                    sys.stderr.write('MultiLangDaemon reported an invalid state while checkpointing.\n')
                else: # Some other error
                    sys.stderr.write('Encountered an error while checkpointing, error was {e}.\n'.format(e=e))
            time.sleep(self.SLEEP_SECONDS)


    def process_record(self, data):

        tweet_dict = json.loads(data)

        debug_str = 'TWEET TEXT: ', tweet_dict['text']
        self._logger.debug(debug_str)
        return

    def process_records(self, records, checkpointer):
        try:
            for _record in records:
                _data = base64.b16decode(_record.get('data'))
                self.process_record(_data)

        except Exception as e:
            self._logger.error(traceback.format_exc())


    def shutdown(self, checkpointer, reason):
        '''
        Called by a KCLProcess instance to indicate that this record processor should shutdown. After this is called,
        there will be no more calls to any other methods of this record processor.

        :type checkpointer: amazon_kclpy.kcl.Checkpointer
        :param checkpointer: A checkpointer which accepts a sequence number or no parameters.

        :type reason: str
        :param reason: The reason this record processor is being shutdown, either TERMINATE or ZOMBIE. If ZOMBIE,
            clients should not checkpoint because there is possibly another record processor which has acquired the lease
            for this shard. If TERMINATE then checkpointer.checkpoint() should be called to checkpoint at the end of the
            shard so that this processor will be shutdown and new processor(s) will be created to for the child(ren) of
            this shard.
        '''
        try:
            if reason == 'TERMINATE':
                # Checkpointing with no parameter will checkpoint at the
                # largest sequence number reached by this processor on this
                # shard id
                print 'Was told to terminate, will attempt to checkpoint.'
                self.checkpoint(checkpointer, None)
            else: # reason == 'ZOMBIE'
                print 'Shutting down due to failover. Will not checkpoint.'
        except:
            pass

if __name__ == "__main__":
    kclprocess = kcl.KCLProcess(TweetProcessor())
    kclprocess.run()
