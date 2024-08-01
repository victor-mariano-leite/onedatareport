
from abc import ABC, abstractmethod

class RemoteDataHandler(ABC):
    """
    Abstract base class for handling the download of remote data files.

    Methods
    -------
    download(path: str) -> str
        Abstract method to download a remote file to a local path.
    """
    
    @abstractmethod
    def download(self, path: str) -> str:
        """
        Download a remote file to a local path.

        Parameters
        ----------
        path : str
            The remote file path or URI (e.g., S3 URI or HTTP URL).

        Returns
        -------
        str
            The local file path where the remote file has been downloaded.
        """
        pass


class HTTPDataHandler(RemoteDataHandler):
    """
    A class to handle downloading data from HTTP or HTTPS URLs.

    Methods
    -------
    download(path: str) -> str
        Download a file from an HTTP/HTTPS URL to a local path.
    """
    
    def download(self, path: str) -> str:
        """
        Download a file from an HTTP/HTTPS URL to a local path.

        Parameters
        ----------
        path : str
            The HTTP or HTTPS URL of the file to be downloaded.

        Returns
        -------
        str
            The local file path where the file has been downloaded.
        """
        import requests
        local_path = "/tmp/temp_file"
        response = requests.get(path)
        with open(local_path, 'wb') as f:
            f.write(response.content)
        return local_path