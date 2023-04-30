
# Developer notes

:construction: **Currently under construction** :construction:

## License

### Analyze licensing dependencies

```Shell
git clone https://github.com/nexB/scancode-toolkit
cd scancode-toolkit
docker build --tag scancode-toolkit --tag scancode-toolkit:$(git describe --tags) .
docker run -v $PWD/:/project scancode-toolkit -clipeu -n 6 --classify --json-pp /project/scan-result.json /project/data-analysis-platform
```

refs: 
- https://scancode-toolkit.readthedocs.io/en/stable/getting-started/install.html#installation-via-docker
- https://scancode-workbench.readthedocs.io/en/develop/getting-started/index.html#download-and-install






