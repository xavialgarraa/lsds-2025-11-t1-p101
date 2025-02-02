import click
from upload import get_file_info, create_file_in_namenode, upload_blocks
from download import fetch_file_metadata, download_file_blocks


@click.group()
def cli():
    """HDFS File Manager CLI."""
    pass


@cli.command(short_help="Upload a file to HDFS.")
def upload():
    """Upload a file to HDFS."""
    local_path, hdfs_name, size = get_file_info()
    if not local_path or not hdfs_name or not size:
        click.echo("Invalid file information. Please try again.", err=True)
        return

    metadata = create_file_in_namenode(hdfs_name, size)
    if not metadata:
        click.echo(f"Failed to create file metadata for '{hdfs_name}'.", err=True)
        return

    upload_blocks(local_path, hdfs_name, metadata)
    click.echo(f"File '{hdfs_name}' successfully uploaded to HDFS.")


@cli.command(short_help="Download <filename> to <destination> from HDFS.")
@click.argument("filename", metavar="<filename>", type=str)
@click.argument("destination", metavar="<destination>", type=click.Path(writable=True))
def download(filename, destination):
    """Download a file from HDFS.

    Example:
        ad0q0.py download myfile.jpg /home/user/downloads/
    """
    metadata = fetch_file_metadata(filename)
    if not metadata:
        click.echo(f"Metadata not found for '{filename}'.", err=True)
        return

    success = download_file_blocks(filename, metadata, destination)
    if success:
        click.echo(f"File '{filename}' successfully downloaded to '{destination}'.")
    else:
        click.echo("An error occurred while downloading the file.", err=True)


@cli.command(short_help="Get metadata for <filename> from HDFS.")
@click.argument("filename", metavar="<filename>", type=str)
def info(filename):
    """Get file metadata from HDFS.

    Example:
        ad0q0.py info myfile.txt
    """
    metadata = fetch_file_metadata(filename)
    if metadata:
        click.echo("File Metadata:")
        click.echo(metadata)
    else:
        click.echo(f"No metadata found for '{filename}'.", err=True)


if __name__ == "__main__":
    cli()
