package me.jaypatelbond.popcorn.presentation.home

import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.pager.HorizontalPager
import androidx.compose.foundation.pager.rememberPagerState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Refresh
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.hilt.navigation.compose.hiltViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import me.jaypatelbond.popcorn.domain.model.Movie
import me.jaypatelbond.popcorn.presentation.components.ErrorView
import me.jaypatelbond.popcorn.presentation.components.FeaturedMovieCard
import me.jaypatelbond.popcorn.presentation.components.FeaturedMovieShimmer
import me.jaypatelbond.popcorn.presentation.components.MovieCarousel
import me.jaypatelbond.popcorn.presentation.components.MovieCarouselShimmer
import me.jaypatelbond.popcorn.ui.theme.BackgroundDark
import me.jaypatelbond.popcorn.ui.theme.NetflixRed
import me.jaypatelbond.popcorn.ui.theme.TextSecondary

/**
 * Home screen displaying trending and now playing movies.
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun HomeScreen(
    onMovieClick: (Movie) -> Unit,
    viewModel: HomeViewModel = hiltViewModel()
) {
    val uiState by viewModel.uiState.collectAsStateWithLifecycle()

    Scaffold(
        topBar = {
            TopAppBar(
                title = {
                    Text(
                        text = "Popcorn",
                        style = MaterialTheme.typography.headlineSmall,
                        fontWeight = FontWeight.Bold,
                        color = NetflixRed
                    )
                },
                actions = {
                    IconButton(onClick = { viewModel.refresh() }) {
                        Icon(
                            imageVector = Icons.Default.Refresh,
                            contentDescription = "Refresh",
                            tint = MaterialTheme.colorScheme.onBackground
                        )
                    }
                },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = BackgroundDark
                )
            )
        },
        containerColor = BackgroundDark
    ) { paddingValues ->
        HomeContent(
            uiState = uiState,
            onMovieClick = onMovieClick,
            onRetry = { viewModel.refresh() },
            modifier = Modifier.padding(paddingValues)
        )
    }
}

@OptIn(ExperimentalFoundationApi::class)
@Composable
private fun HomeContent(
    uiState: HomeUiState,
    onMovieClick: (Movie) -> Unit,
    onRetry: () -> Unit,
    modifier: Modifier = Modifier
) {
    val scrollState = rememberScrollState()

    Column(
        modifier = modifier
            .fillMaxSize()
            .background(BackgroundDark)
            .verticalScroll(scrollState),
        verticalArrangement = Arrangement.spacedBy(24.dp)
    ) {
        // Offline Banner
        if (uiState.isOffline) {
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .background(MaterialTheme.colorScheme.error)
                    .padding(vertical = 4.dp),
                contentAlignment = Alignment.Center
            ) {
                Text(
                    text = "No Internet Connection - Showing Cached Data",
                    style = MaterialTheme.typography.labelSmall,
                    color = Color.White
                )
            }
        } else {
            Spacer(modifier = Modifier.height(8.dp))
        }

        // Featured Movies Carousel (top 5 trending movies)
        if (uiState.isTrendingLoading && uiState.trendingMovies.isEmpty()) {
            Box(modifier = Modifier.padding(horizontal = 16.dp)) {
                FeaturedMovieShimmer()
            }
        } else if (uiState.trendingMovies.isNotEmpty()) {
            val featuredMovies = uiState.trendingMovies.take(5)
            FeaturedMoviesCarousel(
                movies = featuredMovies,
                onMovieClick = onMovieClick
            )
        } else if (uiState.trendingError != null) {
            ErrorView(
                message = uiState.trendingError,
                onRetry = onRetry,
                isNetworkError = uiState.trendingError.contains("internet", ignoreCase = true)
            )
        }

        // Trending Movies Section
        if (uiState.isTrendingLoading && uiState.trendingMovies.isEmpty()) {
            MovieCarouselShimmer()
        } else if (uiState.trendingMovies.isNotEmpty()) {
            MovieCarousel(
                title = "Trending This Week",
                movies = uiState.trendingMovies,
                onMovieClick = onMovieClick
            )
        }

        // Now Playing Movies Section
        if (uiState.isNowPlayingLoading && uiState.nowPlayingMovies.isEmpty()) {
            MovieCarouselShimmer()
        } else if (uiState.nowPlayingMovies.isNotEmpty()) {
            MovieCarousel(
                title = "Now Playing",
                movies = uiState.nowPlayingMovies,
                onMovieClick = onMovieClick
            )
        } else if (uiState.nowPlayingError != null && uiState.nowPlayingMovies.isEmpty()) {
             // Only show error for now playing if list is empty, otherwise we rely on trending error
             if (uiState.trendingMovies.isNotEmpty()) {
                 // If trending loaded but now playing failed, maybe just a text or nothing?
                 // For now, let's just show the error view if it's the only thing
                 ErrorView(
                    message = uiState.nowPlayingError,
                    onRetry = onRetry,
                    isNetworkError = uiState.nowPlayingError.contains("internet", ignoreCase = true)
                )
             }
        }

        // Popular Movies (second half of trending)
        if (uiState.trendingMovies.size > 10) {
            MovieCarousel(
                title = "Popular Movies",
                movies = uiState.trendingMovies.drop(10),
                onMovieClick = onMovieClick
            )
        }

        Spacer(modifier = Modifier.height(16.dp))
    }
}

/**
 * Featured movies carousel with infinite scrolling and page indicators.
 */
@OptIn(ExperimentalFoundationApi::class)
@Composable
private fun FeaturedMoviesCarousel(
    movies: List<Movie>,
    onMovieClick: (Movie) -> Unit
) {
    if (movies.isEmpty()) return
    
    // Use a large initial page to allow "infinite" scrolling in both directions
    val initialPage = Int.MAX_VALUE / 2
    val pagerState = rememberPagerState(
        initialPage = initialPage,
        pageCount = { Int.MAX_VALUE }
    )

    Column {
        // Horizontal pager with infinite scrolling
        HorizontalPager(
            state = pagerState,
            modifier = Modifier.fillMaxWidth()
        ) { page ->
            // Use modulo to get the actual movie index
            val movieIndex = page.mod(movies.size)
            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 16.dp)
            ) {
                FeaturedMovieCard(
                    movie = movies[movieIndex],
                    onClick = { onMovieClick(movies[movieIndex]) }
                )
            }
        }

        // Page indicators (show actual position in movie list)
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(top = 12.dp),
            horizontalArrangement = Arrangement.Center
        ) {
            val currentIndex = pagerState.currentPage.mod(movies.size)
            repeat(movies.size) { index ->
                val isSelected = currentIndex == index
                Box(
                    modifier = Modifier
                        .padding(horizontal = 4.dp)
                        .size(if (isSelected) 8.dp else 6.dp)
                        .clip(CircleShape)
                        .background(
                            if (isSelected) NetflixRed else TextSecondary.copy(alpha = 0.5f)
                        )
                )
            }
        }
    }
}


