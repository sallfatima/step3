const foundLocations = new Set();
let pauseRequests = false;

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function run(lats, lons, initialRadius) {
  const originalLats = lats.split(',').map((value) => parseFloat(value));
  const originalLngs = lons.split(',').map((value) => parseFloat(value));
  const results = await Promise.all(originalLats.map(async (originalLat, i) => {
    const originalLng = originalLngs[i];
    const panoramaLocation = await findPanoramaWithPause(originalLat, originalLng, initialRadius);
    return panoramaLocation;

  }));
  return results;
}

async function findPanoramaWithPause(originalLat, originalLng, initialRadius) {
  while (pauseRequests) {
    await delay(60000); // Wait for 10 second if paused
  }
  return findPanorama(originalLat, originalLng, initialRadius);
}

async function findPanorama(originalLat, originalLng, initialRadius) {
  var panoramaService = new google.maps.StreetViewService();
  var prefType = google.maps.StreetViewPreference.NEAREST;
  var sourceTypes = [google.maps.StreetViewSource.OUTDOOR, google.maps.StreetViewSource.OUTDOOR];
  var latLon = new google.maps.LatLng(originalLat, originalLng);

  return new Promise(function(resolve, reject) {
    // Initialize attempts counter
    let attempts = 0;
    async function attemptFetch() {
      if (attempts > 2) {
        return resolve('NO_RESULTS');
      }
      panoramaService.getPanorama({ location: latLon, preference: prefType, radius: initialRadius, sources: sourceTypes }, async function (data, status) {
        if (status === 'OK') {
          const panoramaLocation = data.location.latLng.toString();
          const panoramaID = data.location.pano;
          const date = data.imageDate
          const links = data.links
          if (!foundLocations.has(panoramaLocation)) {
            // Add to found locations set
            foundLocations.add(panoramaLocation);
            resolve([panoramaLocation, panoramaID, date, links]);
          } else {
            // Resolve to 'SAME' to continue searching
            resolve('SAME');
          }
        } else {
            if (status == 'ZERO_RESULTS') {
                resolve('NO_RESULTS')
            }
            else {
              // Retry in case of error
              attempts++;

              pauseRequests = true;
              await delay(60000); // Wait for 10 second
              pauseRequests = false; // Resume requests
              attemptFetch();
  //          setTimeout(attemptFetch, 100); // Wait 1 second before retrying
            }
        }
      });
    }
    attemptFetch();
  });
}
