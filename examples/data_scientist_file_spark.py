import io

import boto3
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch import optim
from torch.distributions import Normal
from torch.utils.data import Dataset, DataLoader

from dag_poster import post_hyper_search


class MDN(nn.Module):
    def __init__(self, input_dim, dropout=.2):
        super(MDN, self).__init__()
        self.fc1 = nn.Linear(input_dim, 128)
        self.fc2 = nn.Linear(128, 32)
        self.fc3 = nn.Linear(32, 2)
        self.dropout = dropout

    def forward(self, x):
        x = F.dropout(F.relu(self.fc1(x)), p=self.dropout)
        x = F.dropout(F.relu(self.fc2(x)), p=self.dropout)
        x = self.fc3(x)
        x[:, 1] = F.elu(x[:, 1]) + 1
        return x


class MyDataset(Dataset):
    def __init__(self, bucket, path):
        self.path = path
        self.bucket = bucket
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localstack:4572'
        )
        try:
            self.files = self.s3_client.list_objects_v2(Prefix=path, Bucket=bucket)['Contents']
            self.files = [i['Key'] for i in self.files if i['Key'].endswith('.parquet')]
            self.files.sort()
        except KeyError:
            self.files = []

    def __len__(self):
        return len(self.files)

    def __getitem__(self, item):
        file = self.s3_client.get_object(Bucket=self.bucket, Key=self.files[item])
        df = pd.read_parquet(io.BytesIO(file['Body'].read()))
        return df.values

    @property
    def shape(self):
        # TODO: CHANGE TO READ METADATA
        # ParquetFile(self.files[0]).metadata
        return None, self.__getitem__(0).shape[1]


def train_method(path, lr=1e-3, epochs=50, dropout=.2, batch_size=1024):
    print('TRAINING MODEL')
    dataset = MyDataset(bucket='test', path=path)
    data_loader = DataLoader(dataset)

    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    net = MDN(dataset.shape[1]-1, dropout=dropout).to(device=device)
    optimizer = optim.Adam(net.parameters(), lr=lr)

    for i in range(1, epochs + 1):
        for file_data in data_loader:
            file_data = file_data.squeeze().float()
            x_file, y_file = file_data[:, :-1], file_data[:, -1]
            for j in range(0, x_file.shape[0], batch_size):
                x_batch = x_file[j:j+batch_size].to(device=device)
                y_batch = y_file[j:j+batch_size].to(device=device)
                y_train_pred = net(x_batch).float()

                loss = Normal(y_train_pred[:, 0], y_train_pred[:, 1])
                loss = -loss.log_prob(y_batch).mean()

                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        print(i, loss.item())


if __name__ == '__main__':
    path = 'data/train_data.parquet'
    # train_method(path=path)

    post_hyper_search(
        etl_job_path='examples/pyfiles/etl_job.py',
        train_callable=train_method,
        hyperparameters=dict(lr=[1e-4, 1e-3, 1e-2], epochs=[10, 20, 50], dropout=[.1, .2, .5]),
        n_iterations=15
    )
