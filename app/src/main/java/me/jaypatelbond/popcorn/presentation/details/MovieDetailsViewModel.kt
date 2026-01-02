package me.jaypatelbond.popcorn.presentation.details

import androidx.lifecycle.SavedStateHandle
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
 * ViewModel for the Movie Details screen.
 */
@HiltViewModel
class MovieDetailsViewModel @Inject constructor(
    private val movieRepository: MovieRepository,
    private val networkMonitor: me.jaypatelbond.popcorn.util.NetworkMonitor,
    savedStateHandle: SavedStateHandle
) : ViewModel() {

    private val movieId: Int = checkNotNull(savedStateHandle["movieId"])

    private val _uiState = MutableStateFlow(MovieDetailsUiState())
    val uiState: StateFlow<MovieDetailsUiState> = _uiState.asStateFlow()

    init {
        observeNetworkStatus()
        loadMovieDetails()
    }

    private fun observeNetworkStatus() {
        viewModelScope.launch {
            networkMonitor.isOnline.collect { isOnline ->
                _uiState.update { it.copy(isOffline = !isOnline) }
            }
        }
    }

    private fun loadMovieDetails() {
        viewModelScope.launch {
            movieRepository.getMovieDetails(movieId).collect { result ->
                when (result) {
                    is Resource.Loading -> {
                        _uiState.update {
                            it.copy(
                                isLoading = true,
                                error = null,
                                movie = result.data ?: it.movie,
                                isBookmarked = result.data?.isBookmarked ?: it.isBookmarked
                            )
                        }
                    }
                    is Resource.Success -> {
                        _uiState.update {
                            it.copy(
                                isLoading = false,
                                movie = result.data,
                                isBookmarked = result.data?.isBookmarked ?: false,
                                error = null
                            )
                        }
                    }
                    is Resource.Error -> {
                        _uiState.update {
                            it.copy(
                                isLoading = false,
                                error = result.message,
                                movie = result.data ?: it.movie
                            )
                        }
                    }
                }
            }
        }
    }

    /**
     * Toggle bookmark status for the current movie.
     */
    fun toggleBookmark() {
        viewModelScope.launch {
            movieRepository.toggleBookmark(movieId)
            _uiState.update { it.copy(isBookmarked = !it.isBookmarked) }
        }
    }

    /**
     * Retry loading movie details.
     */
    fun retry() {
        loadMovieDetails()
    }
}
