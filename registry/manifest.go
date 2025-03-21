package registry

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	digest "github.com/opencontainers/go-digest"
)

type ManifestList struct {
	MediaType     string          `json:"mediaType"`
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []ManifestEntry `json:"manifests"`
}

type ManifestEntry struct {
	MediaType string       `json:"mediaType"`
	Digest    string       `json:"digest"`
	Size      int          `json:"size"`
	Platform  PlatformSpec `json:"platform"`
}

type PlatformSpec struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
}

const manifestListMediaType = "application/vnd.docker.distribution.manifest.list.v2+json"

func (registry *Registry) Manifest(repository, reference string) (*schema1.SignedManifest, error) {
	url := registry.url("/v2/%s/manifests/%s", repository, reference)
	registry.Logf("registry.manifest.get url=%s repository=%s reference=%s", url, repository, reference)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", schema1.MediaTypeManifest)
	resp, err := registry.Client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	signedManifest := &schema1.SignedManifest{}
	err = signedManifest.UnmarshalJSON(body)
	if err != nil {
		return nil, err
	}

	return signedManifest, nil
}

func (registry *Registry) ManifestV2(repository, reference string) (*schema2.DeserializedManifest, error) {
	url := registry.url("/v2/%s/manifests/%s", repository, reference)
	registry.Logf("registry.manifest.get url=%s repository=%s reference=%s", url, repository, reference)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", schema2.MediaTypeManifest)
	resp, err := registry.Client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	deserialized := &schema2.DeserializedManifest{}
	err = deserialized.UnmarshalJSON(body)
	if err != nil {
		return nil, err
	}
	return deserialized, nil
}

func (registry *Registry) ManifestDigest(repository, reference string) (digest.Digest, error) {
	url := registry.url("/v2/%s/manifests/%s", repository, reference)
	registry.Logf("registry.manifest.head url=%s repository=%s reference=%s", url, repository, reference)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json")
	resp, err := registry.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	return digest.Parse(resp.Header.Get("Docker-Content-Digest"))
}

func (registry *Registry) ManifestList(repository, reference string) (bool, *ManifestList, error) {
	url := registry.url("/v2/%s/manifests/%s", repository, reference)
	registry.Logf("registry.manifest.head url=%s repository=%s reference=%s", url, repository, reference)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return false, nil, err
	}

	req.Header.Set("Accept", manifestListMediaType)
	resp, err := registry.Client.Do(req)
	if err != nil {
		return false, nil, err
	}
	defer resp.Body.Close()

	manifestBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, nil, err
	}

	var manifestList ManifestList
	if err = json.Unmarshal(manifestBytes, &manifestList); err != nil {
		return false, nil, err
	}

	isManifestList := manifestList.MediaType == manifestListMediaType &&
		manifestList.SchemaVersion == 2 &&
		len(manifestList.Manifests) > 0

	return isManifestList, &manifestList, nil
}

func (registry *Registry) DeleteManifest(repository string, digest digest.Digest) error {
	url := registry.url("/v2/%s/manifests/%s", repository, digest)
	registry.Logf("registry.manifest.delete url=%s repository=%s reference=%s", url, repository, digest)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := registry.Client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}
	return nil
}

func (registry *Registry) PutManifest(repository, reference string, manifest distribution.Manifest) error {
	url := registry.url("/v2/%s/manifests/%s", repository, reference)
	registry.Logf("registry.manifest.put url=%s repository=%s reference=%s", url, repository, reference)

	mediaType, payload, err := manifest.Payload()
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(payload)
	req, err := http.NewRequest("PUT", url, buffer)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", mediaType)
	resp, err := registry.Client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	return err
}

func (m *ManifestList) Arch() []string {
	arch := make([]string, 0)
	for _, manifest := range m.Manifests {
		arch = append(arch, manifest.Platform.Architecture)
	}
	return arch
}

func (m *ManifestList) OS() []string {
	os := make([]string, 0)
	for _, manifest := range m.Manifests {
		os = append(os, manifest.Platform.OS)
	}
	return os
}
