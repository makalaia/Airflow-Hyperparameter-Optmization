import cloudpickle as pickle

from dags.lib.aws import S3


def post_hyper_search(etl_job_path, train_callable, hyperparameters, n_iterations=15):

    s3_client = S3()
    s3_client.create_bucket()

    s3_client.upload_s3_file(etl_job_path, 'functions/etl_job.py')

    # DUMP TRAINING FUNCTION
    train_dict = dict(callable=train_callable, hyperparameters=hyperparameters, n_iterations=n_iterations)
    s3_client.put_object(pickle.dumps(train_dict), 'functions/training.pickle')

