package me.jaypatelbond.popcorn.presentation.home

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import me.jaypatelbond.popcorn.domain.repository.MovieRepository
import me.jaypatelbond.popcorn.util.Resource
import javax.inject.Inject

/**
 * ViewModel for the Home screen.
 * Manages loading trending and now playing movies with proper state handling.
 */
@HiltViewModel
class HomeViewModel @Inject constructor(
    private val movieRepository: MovieRepository,
    private val networkMonitor: me.jaypatelbond.popcorn.util.NetworkMonitor
) : ViewModel() {

    private val _uiState = MutableStateFlow(HomeUiState())
    val uiState: StateFlow<HomeUiState> = _uiState.asStateFlow()

    init {
        observeNetworkStatus()
        loadMovies()
    }

    private fun observeNetworkStatus() {
        viewModelScope.launch {
            networkMonitor.isOnline.collect { isOnline ->
                _uiState.update { it.copy(isOffline = !isOnline) }
            }
        }
    }

    /**
     * Load both trending and now playing movies.
     */
    fun loadMovies() {
        loadTrendingMovies()
        loadNowPlayingMovies()
    }

    private fun loadTrendingMovies() {
        viewModelScope.launch {
            movieRepository.getTrendingMovies().collect { result ->
                when (result) {
                    is Resource.Loading -> {
                        _uiState.update { 
                            it.copy(
                                isTrendingLoading = true,
                                trendingError = null,
                                trendingMovies = result.data ?: it.trendingMovies
                            )
                        }
                    }
                    is Resource.Success -> {
                        _uiState.update { 
                            it.copy(
                                isTrendingLoading = false,
                                trendingMovies = result.data ?: emptyList(),
                                trendingError = null
                            )
                        }
                    }
                    is Resource.Error -> {
                        _uiState.update { 
                            it.copy(
                                isTrendingLoading = false,
                                trendingError = result.message,
                                trendingMovies = result.data ?: it.trendingMovies
                            )
                        }
                    }
                }
            }
        }
    }

    private fun loadNowPlayingMovies() {
        viewModelScope.launch {
            movieRepository.getNowPlayingMovies().collect { result ->
                when (result) {
                    is Resource.Loading -> {
                        _uiState.update { 
                            it.copy(
                                isNowPlayingLoading = true,
                                nowPlayingError = null,
                                nowPlayingMovies = result.data ?: it.nowPlayingMovies
                            )
                        }
                    }
                    is Resource.Success -> {
                        _uiState.update { 
                            it.copy(
                                isNowPlayingLoading = false,
                                nowPlayingMovies = result.data ?: emptyList(),
                                nowPlayingError = null
                            )
                        }
                    }
                    is Resource.Error -> {
                        _uiState.update { 
                            it.copy(
                                isNowPlayingLoading = false,
                                nowPlayingError = result.message,
                                nowPlayingMovies = result.data ?: it.nowPlayingMovies
                            )
                        }
                    }
                }
            }
        }
    }

    /**
     * Refresh all movies.
     */
    fun refresh() {
        loadMovies()
    }
}
