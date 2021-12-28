
/*
 * mdRecieveBeast.h
 *
 *  Created on: 07-Jul-2021
 *      Author: sandeepdas
 */


#ifndef INCLUDE_MDRECIEVE_BEAST_ASYNC_H_
#define INCLUDE_MDRECIEVE_BEAST_ASYNC_H_

#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <atmoic>
#include <exception>
#include "rootCertificates.hpp"


#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>

namespace beast = boost::beast;         // from <boost/beast.hpp>
namespace http = beast::http;           // from <boost/beast/http.hpp>
namespace websocket = beast::websocket; // from <boost/beast/websocket.hpp>
namespace net = boost::asio;            // from <boost/asio.hpp>
namespace ssl = boost::asio::ssl;       // from <boost/asio/ssl.hpp>
using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>



namespace fp
{
	namespace okexmd
	{

		enum WSState
		{
			BEGIN,
			CONNECT,
			CONNECTED,
			SSLHANDSHAKE,
			SSLHANDSHAKE_COMPLETED,
			HANDSHAKE,
			HANDSHAKE_COMPLETED,
			SUBSCRIBE,
			SUBSCRIBED,
			UNSUBSCRIBE,
			UNSUBSCRIBED,
			CLOSED
		};

		// enable_shared_from_this will be useful for creating shared pointers with weak referencing
		class WebSocketMDReceiverBeastAsync : public std::enable_shared_from_this<WebSocketMDReceiverBeastAsync>
        {
		public:
			WebSocketMDReceiverBeastAsync(std::fstream& fs, fp::ds::TestAndSetLock& lock,ssl::context& ctx, boost::asio::io_context* ioc)
			:_fs(fs),_ioc(ioc),_lock(lock),_ctx(ctx)
            {
            }
			__fp_inline void setReceiverName(std::string& recName);
			__fp_inline std::string getReceiverName();
			__fp_inline void createClient(int32_t port, std::string& uri, std::string& handshakePath);
			__fp_inline void recreateClient();
			__fp_inline void unsubcribe();
			__fp_inline void addJsonObject(std::string subscribeObj, std::string unsubscribeObj);
			__fp_inline void setState(bool stateInfo);
			__fp_inline int64_t getIdleTime(const uint64_t currTime);
		    void onConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type ep);
		    void onSSLHandshake(beast::error_code ec);
		    void onHandshake(beast::error_code ec);
		    void onWrite(beast::error_code ec,std::size_t bytes_transferred);
		    void onRead(beast::error_code ec, std::size_t bytes_transferred);
		    void onClose(beast::error_code ec);
            void close();
		private:
			__fp_inline void connect(bool wait = true);
			__fp_inline void subcribe();
			std::atomic<uint64_t> _lastReceiveTime ;
			std::fstream& _fs;
			std::string _uri = "";
			std::string _host = "";
			std::string _port = "";
			std::string _handshakePath = "";
			std::string _receiverName;
			boost::asio::io_context* _ioc= nullptr;
			std::atomic<bool>  _active;
			std::atomic<WSState> _state;

            boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>* _ws = nullptr;
            boost::beast::flat_buffer _buffer;
		    ssl::context& _ctx;
		    std::string _subscribeObj;
		    std::string _unsubscribeObj ;

		};

		__fp_inline void WebSocketMDReceiverBeastAsync::setState(bool stateInfo)
		{
		     _active = stateInfo;
		}

		__fp_inline int64_t WebSocketMDReceiverBeastAsync::getIdleTime(const uint64_t currTime)
		{
			return currTime - _lastReceiveTime;
		}

		__fp_inline void WebSocketMDReceiverBeastAsync::createClient(int32_t port, std::string& uri, std::string& handshakePath)
		{
			_port = std::to_string(port);
			_uri = uri;
			_handshakePath = handshakePath;
	        // Update the host_ string. This will provide the value of the
	        // Host HTTP header during the WebSocket handshake.
	        // See https://tools.ietf.org/html/rfc7230#section-5.4
	        //_uri += ':' + std::to_string(ep.port());

			cout << "URI: " << _uri << ":" << _port << _handshakePath<< "\n";
			_state = BEGIN;
			connect();
		}

		__fp_inline void WebSocketMDReceiverBeastAsync::setReceiverName(std::string& recName)
		{
			_receiverName = recName;
		}

		__fp_inline std::string WebSocketMDReceiverBeastAsync::getReceiverName()
		{
			return _receiverName ;
		}

		__fp_inline void WebSocketMDReceiverBeastAsync::recreateClient()
		{
            try
            {
                if (!_active)
                {
                    cout << "Its marked inactive\n" ;
                    _lastReceiveTime = time();
                    return;
                }
                unsubcribe();
	            // Deleting null ptr doesn't create crash.
                while(_state != CLOSED)
                {
                	usleep(1);
                }
                delete _ws;
	            _ws = nullptr;
	            cout << "Recreating the client\n";
	            connect(false);

            }
    		catch(std::exception const& e)
    		{
    			cout << "Exception thrown:" << e.what() <<std::endl;
    			return ;
    		}
		}

		__fp_inline void WebSocketMDReceiverBeastAsync::connect(bool wait)
		{
			cout << "Starting the receiver for :" << _receiverName << '\n';

			if (!_subscribeObj.size())
			{
				cout << "Subscribe string is empty.\n";
				return;
			}
			_lastReceiveTime = time();
			_ws= new boost::beast::websocket::stream<boost::beast::ssl_stream<boost::beast::tcp_stream>>(boost::asio::make_strand(*_ioc),_ctx);
	        // Look up the domain name
            cout << "Going to resolve \n";
			boost::asio::ip::tcp::resolver resolver(boost::asio::make_strand(*_ioc));
	        auto const results = resolver.resolve(_uri, _port);
	        _state = CONNECT;

            cout << "Going to connect\n";
	        beast::get_lowest_layer(*_ws).expires_after(std::chrono::seconds(30));
	        beast::get_lowest_layer(*_ws).async_connect(
	            results,
	            beast::bind_front_handler(
	                &WebSocketMDReceiverBeastAsync::onConnect,
	                shared_from_this()));

			// This is mandatorily added so that user of this interface
			// doesn't forget to do so before creating another websocket.
			// As per exchange only one websocket is allowed per second.
			if (wait)
			{
				sleep(1);
			}
			_active =true;

		}

	    void WebSocketMDReceiverBeastAsync::onConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type ep)
	    {
	        if(ec)
	        {
	        	cout << "Connect: " << ec.message() << "\n";
	        	return;
	        }
            cout << "Connected\n";
	        _state = CONNECTED;

	        // Set a timeout on the operation
	        beast::get_lowest_layer(*_ws).expires_after(std::chrono::seconds(30));

	        // Set SNI Hostname (many hosts need this to handshake successfully)
	        if(! SSL_set_tlsext_host_name(
	                _ws->next_layer().native_handle(),
					_uri.c_str()))
	        {
	            ec = beast::error_code(static_cast<int>(::ERR_get_error()),
	                net::error::get_ssl_category());
	            cout << "Connect: " << ec.message() << "\n";
	            return;
	        }

	        // Update the host_ string. This will provide the value of the
	        // Host HTTP header during the WebSocket handshake.
	        // See https://tools.ietf.org/html/rfc7230#section-5.4
	        _host =  _uri + ':' + std::to_string(ep.port());
	        _state = SSLHANDSHAKE;
	        // Perform the SSL handshake
	        _ws->next_layer().async_handshake(
	            ssl::stream_base::client,
	            beast::bind_front_handler(
	                &WebSocketMDReceiverBeastAsync::onSSLHandshake,
	                shared_from_this()));
	    }

	    void WebSocketMDReceiverBeastAsync::onSSLHandshake(beast::error_code ec)
	    {
	    	if (ec)
	    	{
	    		cout << "SSLHandshake: " << ec.message() << "\n";
	    		return;
	    	}
            cout << "SSL Handshake done\n";
	        _state = SSLHANDSHAKE_COMPLETED;
	        // Turn off the timeout on the tcp_stream, because
	        // the websocket stream has its own timeout system.
	        beast::get_lowest_layer(*_ws).expires_never();

	        // Set suggested timeout settings for the websocket
	        websocket::stream_base::timeout options = websocket::stream_base::timeout::suggested(beast::role_type::client);
	        options.idle_timeout = std::chrono::seconds{10};
	        options.keep_alive_pings = true;
	        _ws->set_option(options);
	        // Set a decorator to change the User-Agent of the handshake
	        _ws->set_option(websocket::stream_base::decorator(
	            [](websocket::request_type& req)
	            {
	                req.set(http::field::user_agent,
	                    std::string(BOOST_BEAST_VERSION_STRING) +
	                        " websocket-client-async-ssl");
	            }));
	        _state = HANDSHAKE;
	        // Perform the websocket handshake
	        _ws->async_handshake(_host, _handshakePath,
	            beast::bind_front_handler(
	                &WebSocketMDReceiverBeastAsync::onHandshake,
	                shared_from_this()));
	    }

	    void WebSocketMDReceiverBeastAsync::onHandshake(beast::error_code ec)
	    {
	        if(ec)
	        {
	    		cout << "Handshake: " << ec.message() << "\n";
	    		return;
	        }
            LOG_INIT("Handshake done\n");
	        _state = HANDSHAKE_COMPLETED;
	        subcribe();

	    }

	    void WebSocketMDReceiverBeastAsync::onWrite(beast::error_code ec,std::size_t bytesTransferred)
	    {
	    	if (ec)
	    	{
	    		cout << "OnWrite: " << ec.message() << "\n";
	    		_state = CLOSED;
	    		return;
	    	}


	    	if (_state == SUBSCRIBE)
	    	{
	    		_state = SUBSCRIBED;
		        cout << "Subscribed:" << _subscribeObj<< '\n';
	    	}
	    	else if (_state == UNSUBSCRIBE)
	    	{
	    		_state = UNSUBSCRIBED;
		        cout << "UnSubscribed:" << _unsubscribeObj<< '\n';
		        if (_ws->is_open())
		        {
		            _ws->async_close(websocket::close_code::normal,
		                beast::bind_front_handler(
		                    &WebSocketMDReceiverBeastAsync::onClose,
		                    shared_from_this()));
		        }
	    	}
	    	cout << "Bytes sent:" << bytesTransferred << '\n';
	    }

		__fp_inline void WebSocketMDReceiverBeastAsync::subcribe()
		{
	        _state = SUBSCRIBE;
			_buffer.clear();
	        _ws->async_read(
	            _buffer,
	            beast::bind_front_handler(
	                &WebSocketMDReceiverBeastAsync::onRead,
	                shared_from_this()));
	        // Send the message
	        _ws->async_write(
	            net::buffer(_subscribeObj),
	            beast::bind_front_handler(
	                &WebSocketMDReceiverBeastAsync::onWrite,
	                shared_from_this()));

		}

		__fp_inline void WebSocketMDReceiverBeastAsync::unsubcribe()
		{
			if (_state == CLOSED)
			{
				return;
			}
			_state = UNSUBSCRIBE;
	        // Send the message
	        _ws->async_write(
	            net::buffer(_unsubscribeObj),
	            beast::bind_front_handler(
	                &WebSocketMDReceiverBeastAsync::onWrite,
	                shared_from_this()));

		}

        void WebSocketMDReceiverBeastAsync::close()
        {
            _ws->async_close(websocket::close_code::normal,
                    beast::bind_front_handler(
                        &WebSocketMDReceiverBeastAsync::onClose,
                        shared_from_this()));
        }
		void WebSocketMDReceiverBeastAsync::onRead(beast::error_code ec, std::size_t bytes_transferred)
		{
			if (ec)
			{
				cout << "Error in Read:" << ec.message() << std::endl;
				if (_ws != nullptr && _ws->is_open())
				{
					close();
				}
				return;
			}
            _lastReceiveTime = time();
            // We'll write one json record as one line in the file.
            _fs << _lastReceiveTime << ","<< boost::beast::buffers_to_string(_buffer.data())  <<'\n';
            _buffer.clear();
            if (fp::sys::SignalHandler::isTerminated())
            {
            	cout << "Read terminated\n";
            	return;
            }
	        _ws->async_read(
	            _buffer,
	            beast::bind_front_handler(
                    &WebSocketMDReceiverBeastAsync::onRead,
                    shared_from_this()));

		}

		void WebSocketMDReceiverBeastAsync::onClose(beast::error_code ec)
		{
			_state = CLOSED;
			delete _ws;
			_ws = nullptr;
			if (ec)
			{
				cout << "Close: " << ec.message() << "\n";
				return;
			}

		}
		void WebSocketMDReceiverBeastAsync::addJsonObject(std::string subscribeObj, std::string unsubscribeObj)
		{
			_subscribeObj = subscribeObj;
			_unsubscribeObj = unsubscribeObj;
		}



	}
}



#endif /* INCLUDE_MDRECIEVE_BEAST_ASYNC_H_ */
