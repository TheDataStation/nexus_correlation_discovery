from setuptools import setup, find_packages

if __name__ == '__main__':
    # found_packages = find_packages(include=["nexus", "nexus.*"])
    # print("Packages found:", found_packages)  # Debugging step
    setup(
        name="nexus-corr-discovery",
        version="0.0.3dev3",
        description="Correlation discovery over collections of spatio-temporal datasets",
        long_description=open("README.md").read(),
        long_description_content_type="text/markdown",
        packages=find_packages(include=["nexus", "nexus.*"]),
        install_requires=[
            "wheel",
            "dill==0.3.8",
            "duckdb==1.0.0",
            "factor_analyzer==0.5.1",
            "geopandas==0.14.4",
            "mmh3==4.1.0",
            "networkx==3.1",
            "numpy==1.24.4",
            "pandas==2.0.3",
            "pingouin==0.5.4",
            "psycopg2-binary",
            "pyvis==0.3.2",
            "PyYAML==6.0.1",
            "Requests==2.31.0",
            "scikit_learn",
            "scipy==1.14.1",
            "Shapely==2.0.3",
            "SQLAlchemy==2.0.25",
            "tqdm==4.66.1",
            "ipywidgets==7.6.5",
        ],
        python_requires=">=3.8",
        include_package_data=True,
        package_data={
            "nexus.data_prep": ["*.json"],
        },
    )